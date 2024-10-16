import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1729120935905 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": -1, "withHeader": True, "separator": ","}, connection_type="s3", format="csv", connection_options={"paths": ["s3://landing-phcj/data/"], "recurse": True}, transformation_ctx="AmazonS3_node1729120935905")

# Script generated for node SQL Query
SqlQuery0 = '''
select 
    id_
    ,name
    ,city 
    ,FLOOR(DATEDIFF(CURRENT_DATE(), date_) / 365.25) AS age
from myDataSource
'''
SQLQuery_node1729121802144 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":AmazonS3_node1729120935905}, transformation_ctx = "SQLQuery_node1729121802144")

# Script generated for node Amazon S3
AmazonS3_node1729121050026 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1729121802144, connection_type="s3", format="csv", connection_options={"path": "s3://landing-phcj/transformed_data/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1729121050026")

job.commit()
