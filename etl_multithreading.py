import sys
import logging
import datetime as dt
from pyspark.context import SparkContext
from pyspark import SparkConf
from pyspark.sql.types import StringType
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from utility.modules_aws_services import AwsServiceModule
from module_common_operations import CommonOperations
from pyspark.sql.functions import lit, regexp_replace, col
import pg8000
import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed
from traceback import format_exc



# config

todays_dt            = dt.datetime.today().strftime('%Y-%m-%d')
current_datetime     = dt.datetime.now()
gmt                  = dt.timezone.utc
current_datetime_gmt = dt.datetime.now(gmt)

# flake8:noqa: E221
# Create objects of helper modules
aws_object     = AwsServiceModule()
common_methods = CommonOperations()


def create_spark_context():
    """
    Create spark_session and glue context for glue job execution
    Args:
        None
    Returns:
        return glueContext, sparkSession and glue job instance
    """
    try:
        conf = SparkConf()
        conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
        conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        sc = SparkContext.getOrCreate(conf=conf)
        glue_context = GlueContext(sc)
        spark = glue_context.spark_session
        print("spark session creation : COMPLETED")

    except Exception as exp:
        raise exp

    return glue_context, spark

#-----------------------------
glueContext, spark = create_spark_context()
schema_bucket_nm="test-youtube-datalake-proj"
schema_file_key="utility/app/app_schema.yaml"
src_bucket_nm="test-youtube-raw-confidential"
strd_bucket_nm="test-youtube-structured-confidential" 
# Redshift connection details
redshift_host = 'youtube-rssls-workgroup.123456789.us-east-1.redshift-serverless.amazonaws.com'
redshift_port = 5439
redshift_dbname = 'rssls_test_d01'
redshift_user = "db_admin"
redshift_password = 'test123'

def multi_processing(file_type,src_file_path,src_file_nm,src_file_s3_uri,table_name,schema_name,source_applicaton_name,redshift_table_name,src_file_key,strd_copy_path):
    try:
        defined_schema = common_methods.get_schema(bucket_nm=schema_bucket_nm, file_key=schema_file_key, tbl_nm=table_name)
        print(f" {file_type} --> Raw zone activities : STARTED")
        raw_df = common_methods.create_raw_dataframe(src_file_s3_uri, spark, defined_schema)
        raw_record_count = raw_df.count()
        print(f" {file_type} --> raw dataframe record count : {raw_record_count}")
        raw_df = common_methods.update_column_name_to_lowercase(raw_df)
      
        print(f" {file_type} --> Schema for raw dataframe")
        raw_df.printSchema()
        # Define the pattern to keep ASCII printable characters, carriage returns, and newlines
        pattern = r'[^\x20-\x7E\r\n]'
        # Apply the regex pattern to each string column
        raw_df = raw_df.select([regexp_replace(col(c), pattern, ' ').alias(c) if raw_df.schema[c].dataType == StringType() else col(c) for c in raw_df.columns])
        print(f" {file_type} --> Raw zone activities : COMPLETED")
    
        print(f" {file_type} --> Structured zone activities : STARTED")
        print(f" {file_type} --> Adding user defined column in structured dataframe")
        strd_df = common_methods.add_user_defined_columns(raw_df, table_name)
        strd_df = common_methods.implement_dataquality_rules(src_file_s3_uri, strd_df)
        strd_df.cache() # it is a transformation and dataframe will cache in next action
    
        dedup_raw_record_count = strd_df.count()
        print(f" {file_type} --> structured dataframe record count after DQ rules processing : {dedup_raw_record_count}")
        strd_count = common_methods.write_structured_parquet(strd_bucket_nm, src_file_key, strd_df, spark)
    
        print(f" {file_type} --> Structured zone activities : COMPLETED")
    
        print(f" {file_type} --> redshift zone activities : STARTED")

        # Establishing connection to Redshift
        conn = pg8000.connect(
            host=redshift_host,
            port=redshift_port,
            database=redshift_dbname,
            user=redshift_user,
            password=redshift_password
        )
        
        # Creating cursor
        cursor = conn.cursor()
        
        # Creating the COPY command
        copy_command = f"""
        COPY {redshift_table_name}
        FROM '{strd_copy_path}'
        IAM_ROLE 'arn:aws:iam::123456789:role/delegate-admin-ghw-redshift-role'
        FORMAT AS PARQUET
        """
        print(f" {file_type} --> copy_command : {copy_command}")
        
        # Executing the COPY command
        cursor.execute(copy_command)
        conn.commit()
        
        # Closing the connection
        cursor.close()
        conn.close()


        print(f" {file_type} --> redshift zone activities : COMPLETED")
    
        
    except Exception as exp:
        print("Error Occurred in Multithreading code :- ", format_exc())
        raise exp    

def main():
    arr=[
        {
            "file_type":"file_type_1",
            "src_file_key":"db2/incremental/app/file_type_1/2024-09-12/file_type_13.txt",
            "redshift_table_name":"app_db.file_type_1",
            "strd_copy_path":"s3://test-youtube-structured-confidential/db2/incremental/app/file_type_1/2024-09-12/file_type_13.txt/",
        },
        {
            "file_type":"file_type_2",
            "src_file_key":"db2/incremental/app/file_type_2/2024-11-05/file_type_2.txt",
            "redshift_table_name":"app_db.file_type_2",
            "strd_copy_path":"s3://test-youtube-structured-confidential/db2/incremental/app/file_type_2/2024-11-05/file_type_2.txt/",
        },
        {
            "file_type":"file_type_3",
            "src_file_key":"db2/incremental/app/file_type_3/2024-11-26/file_type_3.txt",
            "redshift_table_name":"app_sermnt.file_type_3",
            "strd_copy_path":"s3://test-youtube-structured-confidential/db2/incremental/app/file_type_3/2024-11-26/file_type_3.txt/",
        },
    ]

    with ThreadPoolExecutor() as executor:
        sts_client = boto3.client('sts')
        for rec in arr:
            file_type = rec["file_type"]
            src_file_key = rec["src_file_key"]
            redshift_table_name = rec["redshift_table_name"]
            strd_copy_path = rec["strd_copy_path"]
            src_file_path = '/'.join(src_file_key.split('/')[:-1])
            src_file_nm = src_file_key.split('/')[-1]
            src_file_s3_uri = f's3://{src_bucket_nm}/{src_file_key}'
            table_name = src_file_key.split('/')[-3]
            schema_name = src_file_key.split('/')[-4]
            source_application_name = src_file_key.split('/')[0]
            executor.submit(multi_processing,file_type, src_file_path, src_file_nm, src_file_s3_uri, table_name, schema_name, source_application_name, redshift_table_name, src_file_key, strd_copy_path)
 

if __name__ == "__main__": 
    main()
    print('Job execution ended')
    
