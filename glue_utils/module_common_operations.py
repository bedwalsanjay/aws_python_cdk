# flake8: noqa: E501
# pylint: disable=import-error
"""
This file contains all the methods used in glue jobs available in current stack.
Code for all glue jobs are similar and references this file with appropriate value
This page is developed by Sanjay Bedwal
"""
# pylint: disable=import-error
import datetime as dt
import json
import boto3
from traceback import format_exc
from botocore.exceptions import ClientError
import yaml
from pyspark.sql.types import *
from pyspark.sql.functions import lit, date_format, col, to_date, regexp_replace
from utility.tdaas_dq_functions import DataQuality
from utility.modules_aws_services import AwsServiceModule
import pg8000

aws_object = AwsServiceModule()
current_datetime = dt.datetime.now()

class CommonOperations:
    """
    This class contains common methods whoch can be used across all glue jobs in current module

    Functions Avaiable :
    1. Get schema for given table: get_schema
    2. Create raw data frame from input file: create_raw_dataframe
    3. Updates the column name in data frame to lower case: update_column_name_to_lowercase
    4. identify if there is any mismatched column between to column sets: identify_mismatched_column
    5. Add missing columns to given dataframe: add_missing_columns
    6. Implement data quality rule for given data frame: implement_dataquality_rules
    7. Prepare structured data frame from raw data frame: prepare_structured_dataframe
    8. Add additional user defined column to data frame: add_user_defined_columns
    9. Write parquet file to structured data frame: write_structured_parquet
    10. Write data frame to redshift database: write_to_redshift
    11. Update dynamo db table with glue logs: update_audit_table
    12. Handle exceptions for glue job: handle_exception

    """
    gmt = dt.timezone.utc
    current_datetime_gmt  = dt.datetime.now(gmt)

    def get_schema(self, bucket_nm: str, file_key: str, file_type: str):
        '''
            Function to return spark schema
        '''
        print(
            f"function [set_schema] : extracting schema from yaml file for {file_type}")
        response = aws_object.get_object(
            bucket_name=bucket_nm, object_key=file_key)
        try:
            data = yaml.safe_load(response["Body"])
            schema = data[file_type]['schema']
            spark_schema = eval(schema)

        except Exception as exc:
            raise exc

        return spark_schema

    def create_raw_dataframe(self, s3_file_url: str, spark, defined_schema):
        """
        create raw dataframe from s3 file url

        Args:
            s3_file_url: s3 file url from which raw data frame will be created

        Returns:
            dataFrame : return raw dataframe after reading input file
        """

        try:
            # Read data from CSV file          
            data_frame = spark.read \
                .option("header", "false") \
                .option("sep", '|') \
                .option("inferSchema", "false") \
                .option("timestampFormat", "yyyy-MM-dd-HH.mm.ss.SSSSSS") \
                .schema(defined_schema) \
                .csv(s3_file_url)

        except Exception as exp:
            raise exp

        return data_frame

    def update_column_name_to_lowercase(self, data_frame):
        """
        update the column names in dataframe to lowercase

        Args:
            data_frame : dataframe where column name needs to be updated to lowercase

        Returns:
            dataFrame : Returns input data frame after renaming all the column names to lower case
        """

        for column in data_frame.columns:
            data_frame = data_frame.withColumnRenamed(
                column, column.strip("\r\t\n").lower())

        return data_frame

    def identify_mismatched_column(self, defined_columns: list, input_columns: list):
        """
        Notifiy if there is a mismatch in column between defined schema and incoming raw dataframe

        Args:
            defined_columns : columns mentioned in defined schema
            input_columns : columns  present in incoming raw dataframe

        Returns:
            returns extra column and missing columns
        """

        # Identify mismatched columns and send notification

        columns_extra = list(
            sorted(set(input_columns)-set(defined_columns)))
        columns_missing = list(
            sorted(set(defined_columns)-set(input_columns)))
        print(f"missing columns are : {columns_extra}")
        print(f"extra column are : {columns_missing}")

        return columns_extra, columns_missing

    def add_missing_columns(self, defined_columns: list, input_columns: list, data_frame):
        """
        Notifiy if there is a mismatch in column between defined schema and incoming raw dataframe

        Args:
            defined_columns : columns mentioned in defined schema
            input_columns : columns  present in incoming raw dataframe
            data_frame : dataframe where missing columns needs to be added

        Returns:
            dataFrame : Returns input data frame after adding missing columns
        """

        missing_columns = list(
            sorted(set(defined_columns)-set(input_columns)))

        # Add missing columns to dataframe
        if missing_columns:
            for column in missing_columns:
                data_frame = data_frame.withColumn(
                    column, lit("").cast(StringType()))

        return data_frame

    def implement_dataquality_rules(self, s3_file_url: str, data_frame):
        # Apply DataQuality rules
        """
        Data Quality checks must be in order specified as below and shouldn't be changed
        1. Fill null with blanks
        2. Trim spaces presnt in string
        3. Drop duplicate records from data frame
        """
        print('Executing DQ Rules')

        try:
            dq = DataQuality(data_frame)
            processed_df = dq.fill_nulls_blanks()
            string_columns = dq.get_string_columns()
            processed_df = dq.trim_spaces(string_columns)
            processed_df = processed_df.dropDuplicates()

        except Exception as exc:
            print("Error occurred while performaing data quality checks :- ", format_exc())
            raise exc

        print(
            f'DQ and Audit transformations completed for {s3_file_url}.')

        return processed_df

    def prepare_structured_dataframe(self, data_frame, col_names: list, col_data_types: list):
        """
        prepare stcuctured dataframe from raw dataframe with appropriate schema

        Args:
            data_frame: raw dataframe from which structured data frame is created

        Returns:
            dataFrame: structured dataframe with appropriate schema
        """

        # Prepare structured dataframe by casting columns to appropriate datatypes

        for column, dtype in zip(col_names, col_data_types):
            if dtype == DateType():
                data_frame = data_frame.withColumn(column, to_date(col(column), "M/d/yyyy"))
                data_frame = data_frame.withColumn(column, date_format(col(column), "yyyy-MM-dd"))

        select_cols = [data_frame[col].cast(
            dtype) for col, dtype in zip(col_names, col_data_types)]
        print(select_cols)       
        structured_df = data_frame.select(*select_cols)

        return structured_df

    def add_user_defined_columns(self, data_frame, file_type: str):
        """
        Adds user defined column to structured dataframe

        Args:
            data_frame: raw dataframe from where user defined column needs to be added
            file_type: file or table identifier

        Returns:
            dataFrame: structured dataframe with user defined columns
        """
        data_frame = data_frame.withColumn("rec_ld_gts", lit(self.current_datetime_gmt))
        match file_type:
            case "file_type_1":
                data_frame = data_frame.withColumn("typ_desc", regexp_replace(col("typ_desc"), r"\s{5}.*", ""))
            case _:
                pass
        return data_frame

    def write_structured_parquet(self, strd_bucket_name: str, file_path: str, data_frame, spark):
        """
        create structured parquet file in s3 bucket

        Args:
            strd_bucket_name: structured bucket name where parquet file must be created
            file_path: file path where parquet file is stored
            data_frame: data frame which will be written to parquet fil

        Returns:
            int: count of structured record
        """

        try:
            # Write to S3 structured zone
            print(" writing parquet files in structured path : STARTED")
            data_frame.write.mode('overwrite').parquet(f's3://{strd_bucket_name}/{file_path}')
            print(" writing parquet files in structured path : COMPLETED")

            # Get structured record count
            print("calculating count of parquet file STARTED")
            record_count = spark.read.parquet(f's3://{strd_bucket_name}/{file_path}').count()
            print("calculating count of parquet file COMPLETED")
            print(f"record_count of structured parquet files : {record_count}")

        except Exception as exp:
            raise exp

        return record_count

    def get_secret(self, aws_secret_name: str):
        """
        insert structured record into redshift

        Args:
            aws_secret_name: secret manager where redshift details are stored

        Returns:
            string: returns the secret stored in secret manager
        """

        try:
            sm_client = boto3.client('secretsmanager', region_name="ap-south-1")
            secret_response = sm_client.get_secret_value(
                SecretId=aws_secret_name
            )
            secret_details = secret_response['SecretString']

            secret_json = json.loads(secret_details)

        except ClientError as e:
            raise e

        return secret_json

    def write_to_redshift(self, secret_manager_name: str, tbl_name: str,
                          copy_source_path,redshift_iam_role_arn):
        """
        Copy data from s3 parquet files into redshift table

        Args:
            secret_manager_name: secret manager where redshift details are stored
            tbl_name: redshift table name where data needs to be inserted
            copy_source_path: s3 path where the  parquet files resides to be copied into redshift table
			redshift_iam_role_arn: role used for copying data from s3 into redshift

        Returns:
            None
        """
        try:
            # get redshift database details from secret manager
            options = self.get_secret(secret_manager_name)			
		   # Establishing connection to Redshift
            conn = pg8000.connect(
				host	=options["host"],
				port	=options["url"].split(':')[-1].split('/')[0],
				database=options["dbname"],
				user	=options["username"],
				password=options["password"]
			)

			# Creating cursor
            cursor = conn.cursor()

            truncate_command = f"TRUNCATE TABLE {tbl_name};"
            print(f"Executing Truncate command: {truncate_command}")
            cursor.execute(truncate_command)

            copy_command = f"""
            COPY {tbl_name}
            FROM '{copy_source_path}'
            IAM_ROLE '{redshift_iam_role_arn}'
            FORMAT AS PARQUET
            """
            print(f" {file_type} --> Executing copy command : {copy_command}")
            cursor.execute(copy_command)
            print(f" {file_type} --> Executing copy command completed")

            conn.commit()
            # Closing the connection
            cursor.close()
            conn.close()

        except Exception as exp:
            raise exp

    def update_audit_table(self, audit_table_name: str,
                           glue_job_run_id: str, s3_file_uri: str,
                           status_message: str, stage: str,
                           error_message: str = ""):  # pragma: no cover
        """
        update audit table to store glue status

        Args:
            status_message: status message of current glue job
            stage: current zone in glue job
            error_message: error message if there is any expection

        Returns:
            None
        """

        # update audit table
        aws_object.update_audit_table(
            table_nm=audit_table_name,
            pkey=s3_file_uri,
            data_dict={
                "job_run_id": glue_job_run_id,
                "date_processed": current_datetime.strftime('%Y%m%d'),
                "status": status_message,
                "stage": stage,
                "error message": error_message}
        )

    def handle_exception(self, sns_arn: str, error_subject: str, audit_table_name: str,
                         glue_job_run_id: str, s3_file_uri: str,
                         error_message: str, glue_stage: str, error: str = ""):  # pragma: no cover
        """
        logs and notifies user when an exception occurs

        Args:
            error_subject: subject to send SNS notification
            error_message: body of SNS notification
            status: status message

        Returns:
            None
        """

        aws_object.publish_to_sns_topic(
            topic_arn=sns_arn,
            subject=error_subject,
            message=error_message
        )

        print(error_message)

        # update audit table
        aws_object.update_audit_table(
            table_nm=audit_table_name,
            pkey=s3_file_uri,
            data_dict={
                "job_run_id": glue_job_run_id,
                "date_processed": current_datetime.strftime('%Y%m%d'),
                "status": "Failed at glue",
                "stage": glue_stage,
                "error message": error}
        )

# End-of-file (EOF)
