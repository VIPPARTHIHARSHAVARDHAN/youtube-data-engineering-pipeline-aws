"""
AWS Glue ETL Job

Purpose:
- Read YouTube raw_statistics CSV files from Amazon S3.
- Apply schema mapping and data type conversions.
- Perform basic data quality validation.
- Convert CSV data to Parquet format.
- Store the transformed data in the cleansed S3 bucket.
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1783399978871 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://de-on-youtube-raw-useast1-029289897989/youtube/raw_statistics/"], "recurse": True}, transformation_ctx="AmazonS3_node1783399978871")

# Script generated for node Change Schema
ChangeSchema_node1783400024549 = ApplyMapping.apply(frame=AmazonS3_node1783399978871, mappings=[("video_id", "string", "video_id", "string"), ("trending_date", "string", "trending_date", "string"), ("title", "string", "title", "string"), ("channel_title", "string", "channel_title", "string"), ("category_id", "string", "category_id", "long"), ("publish_time", "string", "publish_time", "string"), ("tags", "string", "tags", "string"), ("views", "string", "views", "long"), ("likes", "string", "likes", "long"), ("dislikes", "string", "dislikes", "long"), ("comment_count", "string", "comment_count", "long"), ("thumbnail_link", "string", "thumbnail_link", "string"), ("comments_disabled", "string", "comments_disabled", "boolean"), ("ratings_disabled", "string", "ratings_disabled", "boolean"), ("video_error_or_removed", "string", "video_error_or_removed", "boolean"), ("description", "string", "description", "string")], transformation_ctx="ChangeSchema_node1783400024549")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1783400024549, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1783397910982", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1783400043288 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1783400024549, connection_type="s3", format="glueparquet", connection_options={"path": "s3://de-on-youtube--cleansed/youtube/raw_statistics/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1783400043288")

job.commit()
