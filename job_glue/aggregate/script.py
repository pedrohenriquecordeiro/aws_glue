import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

def sparkAggregate(glueContext, parentFrame, groups, aggs, transformation_ctx) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs) if len(groups) > 0 else parentFrame.toDF().agg(*aggsFuncs)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1729120935905 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": -1, "withHeader": True, "separator": ","}, connection_type="s3", format="csv", connection_options={"paths": ["s3://landing-phcj/data/"], "recurse": True}, transformation_ctx="AmazonS3_node1729120935905")

# Script generated for node Aggregate
Aggregate_node1729120997870 = sparkAggregate(glueContext, parentFrame = AmazonS3_node1729120935905, groups = ["city"], aggs = [["id_", "count"]], transformation_ctx = "Aggregate_node1729120997870")

# Script generated for node Amazon S3
AmazonS3_node1729121050026 = glueContext.write_dynamic_frame.from_options(frame=Aggregate_node1729120997870, connection_type="s3", format="csv", connection_options={"path": "s3://landing-phcj/transformed_data/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1729121050026")

job.commit()
