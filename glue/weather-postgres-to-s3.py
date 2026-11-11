import sys
import boto3

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "CONNECTION_NAME",
        "SOURCE_TABLE",
        "S3_BUCKET",
        "OUTPUT_PREFIX",
        "WATERMARK_KEY",
    ],
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

bucket = args["S3_BUCKET"]
watermark_key = args["WATERMARK_KEY"]
output_path = f"s3://{bucket}/{args['OUTPUT_PREFIX'].strip('/')}/"
connection_name = args["CONNECTION_NAME"]
source_table = args["SOURCE_TABLE"]

s3 = boto3.client("s3")

try:
    response = s3.get_object(Bucket=bucket, Key=watermark_key)
    last_id = int(response["Body"].read().decode("utf-8").strip())
except s3.exceptions.NoSuchKey:
    last_id = 0
except Exception as exc:
    print(f"Unable to read watermark: {exc}")
    raise

sample_query = (
    f"SELECT * FROM {source_table} WHERE id > {last_id}"
)

source = glue_context.create_dynamic_frame.from_options(
    connection_type="postgresql",
    connection_options={
        "useConnectionProperties": True,
        "connectionName": connection_name,
        "dbtable": source_table,
        "sampleQuery": sample_query,
        "enablePartitioningForSampleQuery": False,
    },
    transformation_ctx="postgres_source",
)

df = source.toDF()
record_count = df.count()

if record_count == 0:
    print("No new records found.")
    job.commit()
else:
    max_id = df.agg({"id": "max"}).collect()[0][0]

    (
        df.write
        .mode("append")
        .format("parquet")
        .save(output_path)
    )

    s3.put_object(
        Bucket=bucket,
        Key=watermark_key,
        Body=str(max_id).encode("utf-8"),
    )

    print(
        f"Wrote {record_count} records to {output_path}; "
        f"watermark advanced from {last_id} to {max_id}"
    )
    job.commit()
