from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, col, concat_ws, round, current_date, lit , when
import json
import sys


source_path = SparkContext.getOrCreate().getConf().get('spark.sourcepath', 'default_value')
print(source_path)

# source_path="s3://vigneshwaran-rawzone-batch01/vickytestdemo/active.parquet"
# Initialize Spark session
spark = SparkSession.builder.appName("TransformationJob").config("spark.jars.packages","io.delta:delta-core_2.12:2.1.0").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").getOrCreate()

spark.sparkContext.addPyFile("s3://vigneshwaram-landingzone-01/delta-core_2.12-1.2.0.jar")

# Define the configuration file path
config_file_path = "s3://vigneshwaram-landingzone-01/appconfi.json"

from delta.tables import DeltaTable


# Read the configuration from the S3 file using spark.read.json
config_df = spark.read.json(config_file_path, multiLine=True)

# Extract configuration values
destination = config_df.select("destination_bucket").head()[0]
transformations = config_df.select("transformations").first()[0].asDict()
# source = config_df.select("source").first()[0]
source=[]
source.append(source_path)
partitions=config_df.select("partition_columns").first()[0]

filename=source_path.split('/')[-1]
print(filename)
deltatablepath="s3://vigneshwaran-stagingzone-01/deltatable/"

#for retrieving the actives parquet file
source_df=""

# Iterate over source buckets
for source_bucket in source:
    print(f"Processing bucket: {source_bucket}")

    # Read Parquet file from source bucket
    df = spark.read.parquet(source_bucket)

    # Apply transformations
    for column, transformation in transformations.items():
        if column in df.columns:
            if transformation == "sha2":
                name="masked_"+column
                df = df.withColumn(name, sha2(col(column), 256))
            elif "convert to decimal with" in transformation:
                precision = int(transformation.split(" ")[-2])
                df = df.withColumn(column, round(col(column).cast("decimal(10," + str(precision) + ")"), precision))
            elif transformation == "Convert to a comma-separated string":
                df = df.withColumn(column, concat_ws(",", col(column)))

    # Show the first 5 rows of the transformed DataFrame
    print(f"Sample of Transformed DataFrame from {source_bucket}:")
    df.show(5, truncate=False)

    # Save the transformed DataFrame to the destination bucket with partition columns
    file_name = source_bucket.split("/")[-1].split(".")[0]  # Extracting the bucket name without extension
    output_path = f"{destination}/{file_name}_transformed"
    source_df=output_path+"/"
    df.write.partitionBy(*partitions).parquet(output_path, mode="overwrite")
    print(source_df)

def checkDeltaTable():
    try:
        df = spark.read.format("delta").load(deltatablepath)
        df.show(5)
        return True
       
    except Exception as e:
        print(e)
        return False

if filename=="active.parquet":
    actives_parquet_df = spark.read.parquet(source_df)
    actives_column = ["advertising_id", "user_id", "masked_advertising_id", "masked_user_id"]
    actives_df = actives_parquet_df.select(actives_column)
    actives_df = actives_df.withColumn("start_date", current_date())
    actives_df = actives_df.withColumn("end_date", lit("0/0/0000"))

    if checkDeltaTable():
        print("before reading delta")
        delta_table = DeltaTable.forPath(spark, deltatablepath)
        delta_df = delta_table.toDF()
        print("Existing Delta table:")
        delta_df.show(10)

        # Join active_df with delta_df on advertising_id
        joined_df = actives_df.join(delta_df, "advertising_id", "left_outer").select(
            actives_df["*"],
            delta_df.advertising_id.alias("delta_advertising_id"),
            delta_df.user_id.alias("delta_user_id"),
            delta_df.masked_advertising_id.alias("delta_masked_advertising_id"),
            delta_df.masked_user_id.alias("delta_masked_user_id"),
            delta_df.start_date.alias("delta_start_date"),
            delta_df.end_date.alias("delta_end_date")
        )

        print("Joined dataframe:")
        joined_df.show()

        # Filter the joined DataFrame to get rows where user_id is different in delta
        filter_df = joined_df.filter((joined_df["advertising_id"] != joined_df["delta_advertising_id"]) |
                                      ((joined_df["user_id"] != joined_df["delta_user_id"]) & (joined_df["delta_end_date"]=="0/0/0000")))

        print("Filter dataframe:")
        filter_df.show()

        # Add mergekey column for later use
        merge_df = filter_df.withColumn("mergekey", filter_df["advertising_id"])

        print("Merge dataframe:")
        merge_df.show()

        # Create dummy_df for rows where advertising_id matches but user_id is different
        dummy_df = merge_df.filter((merge_df["advertising_id"] == merge_df["delta_advertising_id"]) & (merge_df["user_id"] != merge_df["delta_user_id"])) \
            .withColumn("mergekey", lit(None))

        print("Dummy dataframe:")
        dummy_df.show()

        # Union merge_df and dummy_df to get SCD dataframe
        scd_df = merge_df.union(dummy_df)

        print("SCD dataframe:")
        scd_df.show()
       
   
        # Perform merge operation to update Delta table
        delta_table.alias("delta").merge(
            source=scd_df.alias("source"),
            condition="delta.advertising_id = source.mergekey"
        ).whenMatchedUpdate(
            set={
                "end_date": str("current_date"),
                "flag_active": lit(False)  # Set flag_active to false
            }
        ).whenNotMatchedInsert(
            values={
                "advertising_id": "source.advertising_id",
                "user_id": "source.user_id",
                "masked_advertising_id": "source.masked_advertising_id",
                "masked_user_id": "source.masked_user_id",
                "start_date": "current_date",
                "end_date": lit("0/0/0000"),
                "flag_active": lit(True)  # Set flag_active to true for new records
            }
        ).execute()
        # Show the updated Delta table
        print("Updated Delta table:")
        delta_table.toDF().show(10)
        delta_df=delta_table.toDF()
        delta_df.write.format("delta").mode("overwrite").save(deltatablepath)
        print("After saving")
       

    else:
        deltacolumns = ["advertising_id", "user_id", "masked_advertising_id", "masked_user_id", "start_date", "end_date"]
        delta_df = actives_df
        delta_df = delta_df.withColumn("flag_active", lit(True))
        print("Sample delta table output:")
        delta_df.show(10)
        delta_df.write.format("delta").mode("overwrite").save(deltatablepath)


# Stop the Spark session
spark.stop()