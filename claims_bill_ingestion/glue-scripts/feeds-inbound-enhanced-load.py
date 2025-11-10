import sys
import logging
import json
import base64
import boto3
import pytz
import pymysql
from datetime import datetime
from botocore.exceptions import ClientError
from botocore.client import Config
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit, col, lower, udf, when, expr, concat, count, round,trim,sha2,concat_ws
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from glue_commons import bitx_glue_common as lib
from glue_commons import bitx_feeds_glue_common as feeds_lib


#Configure logging.
logging.basicConfig(level=logging.INFO)
#set the logging level to INFO
logger = logging.getLogger("claimant-billingestion-feeds-inbound-enhanced-load-job")
    
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    's3_bucket',
    'datacatalog_schema_name',
    'datacatalog_config_table_name',
    'db_secret_key',
    'batch_id',
    'env_profile',
    'sns_topic_arn',
    's3_error_attachment_prefix',
    'error_alerts_to_email_ids',
    'lambda_function_nodemailer',
    'input_file_name',
    'feed_type',
    'provider',
    'failed_batch_id',
    'reprocess_flag',
    'step_function_info',
    'step_function_execution_id'
    ])

# Create GlueContext, SparkContext, and SparkSession
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
logger.info("Glue job initialized successfully...")
job_id = args['JOB_RUN_ID']
logger.info(f"job_id => {job_id}")
job_name = args['JOB_NAME']
logger.info(f"job_name => {job_name}")

s3_bucket=args['s3_bucket'].strip()
datacatalog_schema_name=args['datacatalog_schema_name'].strip()
datacatalog_config_table_name=args['datacatalog_config_table_name'].strip()
s3_error_attachment_prefix=args['s3_error_attachment_prefix'].strip()
error_alerts_to_email_ids=args['error_alerts_to_email_ids'].strip()
lambda_function_nodemailer=args['lambda_function_nodemailer'].strip()
db_secret_key=args['db_secret_key'].strip()
batch_id=args['batch_id'].strip()
env_profile=args['env_profile'].strip()
sns_topic_arn=args['sns_topic_arn'].strip()
input_file_name=args['input_file_name'].strip()
feed_type=args['feed_type']
provider=args['provider']
failed_batch_id=args['failed_batch_id']
reprocess_flag = args['reprocess_flag']
step_function_info = args.get('step_function_info')
step_function_execution_id = args.get('step_function_execution_id')

#intitilizations
mode = "append"
status="IN PROGRESS"
step_id=None
failed_step_id=None
is_execution=True
is_reprocess_exec=False
is_no_data_execution=False
is_skip_execution=False
reprocess_valid_list=['COMPLETED','COMPLETED_IN_REPROCESS']

# Generic methods are now imported from bitx_glue_common library
# Feeds-specific methods are now imported from bitx_feeds_glue_common library 

try:
    logger.info(f"Processing STARTED for {feed_type} Enhanced Load...")
    logger.info(f"db_secret_key => {db_secret_key}")
    secrets = lib.get_secret(db_secret_key, 'us-east-1')
    
    # Insert step log and get step_id
    step_id = feeds_lib.feeds_step_logging(secrets, batch_id, step_id, job_name, job_id, 'IN PROGRESS', 'Glue - Feeds Inbound Enhanced Load')
    logger.info(f"Step record inserted with step_id => {step_id}")

    if reprocess_flag.upper().strip()=='N' and batch_id:
        dup_chk=f"""SELECT count(*) as cnt FROM txm_inbound_feeds_logs.batch_inbound_feeds_step_details st
                     INNER JOIN txm_inbound_feeds_logs.batch_inbound_feeds_master m ON m.batch_id=st.batch_id
                     WHERE m.batch_id={batch_id} AND st.step_id <> {step_id} AND st.job_name='{job_name}'""" 
        dup_chkstatus=feeds_lib.run_sql_query(dup_chk, secrets)
        dup_chk_cnt= int(dup_chkstatus[0]['cnt'])
        if(dup_chk_cnt) > 0:
            is_execution=False
            raise Exception("Duplicate Batch!.Batch is already Processed.") 

    if reprocess_flag.upper().strip()=='Y':
        validate_sql=f"""SELECT m.batch_id,st.status,st.step_id,m.file_name,m.feed_type FROM txm_inbound_feeds_logs.batch_inbound_feeds_step_details st
                     INNER JOIN txm_inbound_feeds_logs.batch_inbound_feeds_master m ON m.batch_id=st.batch_id
                     WHERE m.batch_id={failed_batch_id} AND job_name='{job_name}' """
        validation_enhance_status=feeds_lib.run_sql_query(validate_sql, secrets)
        print(f"validation_enhance_status => {validation_enhance_status}")

        if not validation_enhance_status:
            failed_step_id = feeds_lib.feeds_step_logging(secrets, failed_batch_id, failed_step_id, job_name, job_id, status, 'Glue - Feeds Inbound Enhanced Load')
            logger.info(f"Step record inserted with failed_step_id => {failed_step_id}")
            is_reprocess_exec=True
        
        else:
            feed_type=validation_enhance_status[0]['feed_type'].lower()
            file_name=validation_enhance_status[0]['file_name']
            validation_status=validation_enhance_status[0]['status']
            failed_step_id=validation_enhance_status[0]['step_id']
            if feed_type.lower() == 'deleted_invoice':
                if isinstance(validation_enhance_status, list):
                    if validation_status in reprocess_valid_list:
                        print(f"Batch=> {failed_batch_id} with file name =>{file_name} is already processed succesfully in the last run.Terminating Reprocess!")
                        is_execution=False
                        is_reprocess_exec=False
                        status = "SKIP_EXECUTION"
                    else:
                        logger.info(f"Existing batchi_id {failed_batch_id} found for feed {feed_type} with status as {validation_status}. Starting for reprocessing flow.")
                        is_execution=True
                        is_reprocess_exec=True
            
    if is_execution:
        logger.info(f"Reading Feeds Inbound configuration from Data Catalog table => {datacatalog_schema_name}.{datacatalog_config_table_name}...")
        AWSGlueDataCatalog_EnhancedConfig = glueContext.create_dynamic_frame.from_catalog(database=datacatalog_schema_name, table_name=datacatalog_config_table_name, transformation_ctx="AWSGlueDataCatalog_EnhancedConfig")
        AWSGlueDataCatalog_EnhancedConfig.printSchema()
        AWSGlueDataCatalog_EnhancedConfig.show()
        config_df = AWSGlueDataCatalog_EnhancedConfig.toDF()
        config_rec = config_df.filter(lower(col("feed_type")) == feed_type.lower()).first()
        logger.info(f"config_rec => {config_rec}")

        if is_reprocess_exec:
            input_batch_id = failed_batch_id
        else:
            input_batch_id = batch_id

        record_list_df = lib.get_df_db_data_with_query(spark, config_rec["db_read_sql_select"].replace('<INPUT_BATCH_ID>', input_batch_id), secrets)
        record_list_df = record_list_df.cache()
        record_list_df.createOrReplaceTempView(config_rec["df_table_name"])
        record_list_df.printSchema()
        record_list_df.show(5, truncate=False)
        record_list_count = record_list_df.count()
        logger.info(f"Record Count => {record_list_count}")
        header_id = record_list_df.first()["header_id"]
        logger.info(f"header_id => {header_id}")

        if record_list_count > 0:
            logger.info(f"Data Available for Batch ID {input_batch_id}...")
            logger.info(f"feeds inbound validations starts...")

            is_validation = True
            v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            logger.info(f"v_load_timestamp => {v_load_timestamp}")

            logger.info(f'Reading {feed_type} configuration from Data Catalog table => {config_rec["datacatalog_schema_name"]}.{config_rec["datacatalog_validation_table_name"]}...')
            AWSGlueDataCatalog_ValidationTPConfig = glueContext.create_dynamic_frame.from_catalog(database=config_rec["datacatalog_schema_name"], table_name=config_rec["datacatalog_validation_table_name"], transformation_ctx="AWSGlueDataCatalog_ValidationTPConfig")
            AWSGlueDataCatalog_ValidationTPConfig.printSchema()
            AWSGlueDataCatalog_ValidationTPConfig.show()
            validation_config_df = AWSGlueDataCatalog_ValidationTPConfig.toDF()
            validation_config_df.show(3, truncate=False)
            recordCount = validation_config_df.count()
            logger.info(f"{feed_type} config Record Count => {recordCount}")
            validation_config_itr = validation_config_df.orderBy(['order_seq_no']).rdd.toLocalIterator()

            validation_result_df = spark.createDataFrame([], StructType([]))
            validation_result_df.printSchema()
            validation_result_df.show(5, truncate=False)
            recordCount = validation_result_df.count()
            logger.info(f"Validation Result Record Count => {recordCount}")

            for row in validation_config_itr:
                logger.info(f"Processing started for row: {row}")

                #Validating Staging table data and loaded into a dataframe
                validation_df = lib.get_df_db_data_with_query(spark, row["db_validate_sql_select"].replace('<INPUT_HEADER_ID>', str(header_id)), secrets)
                validation_df = validation_df.cache()
                validation_df.createOrReplaceTempView(row["df_table_name"])
                validation_df.printSchema()
                validation_df.show(5, truncate=False)
                recordCount = validation_df.count()
                logger.info(f"Validation result Record Count => {recordCount}")
                
                temp_validation_result_df = validation_df.withColumn("validation_no",lit(row["order_seq_no"])) \
                    .withColumn("error_description",when(col("validation_result")=="FAIL",lit(row["error_desc"])).otherwise(col("validation_result"))) \
                    .select(col("batch_id"),col("header_id"),col("detail_id"),col("invoice_number"),col("field_name_1"),col("field_value_1"),col("field_name_2"),col("field_value_2"),col("validation_no"),col("validation_result"),col("error_description")) \
                    .cache()
                temp_validation_result_df.printSchema()
                temp_validation_result_df.show(5, truncate=False)
                recordCount = temp_validation_result_df.count()
                logger.info(f"Temp Validation result Record Count => {recordCount}")

                temp_validation_error_log_df=temp_validation_result_df.filter(temp_validation_result_df["validation_result"] == "FAIL") \
                    .select(col("batch_id"),col("header_id"),col("detail_id"),col("invoice_number"),col("error_description"),col("field_name_1"),col("field_value_1"),col("field_name_2"),col("field_value_2"),col("validation_no"),col("validation_result")).cache()
                temp_validation_error_log_df.printSchema()
                temp_validation_error_log_df.show(5, truncate=False)
                errorRecordCount = temp_validation_error_log_df.count()
                logger.info(f"Temp Validation Error result Record Count => {errorRecordCount}")

                # Append joined_df to validation_result_df
                if validation_result_df.count() == 0:
                    logger.info(f"validation_result_df Record Count is 0")
                    validation_result_df = temp_validation_result_df.cache()
                else:
                    validation_result_df = validation_result_df.union(temp_validation_result_df).cache()
                validation_result_df.printSchema()
                validation_result_df.show(5, truncate=False)
                recordCount = validation_result_df.count()
                logger.info(f"Validation Result Record Count => {recordCount}")
            windowSpec = Window.partitionBy("detail_id")
            validation_result_df = validation_result_df.withColumn('validation_count', count(col("validation_no")).over(windowSpec))
            validation_result_df.printSchema()
            validation_result_df.show(5, truncate=False)
            recordCount = validation_result_df.count()
            logger.info(f"Validation Result (Added Validation count) Record Count => {recordCount}")
            
            logger.info("Grouping Bill validation errors and Sending mail alerts...")
            validation_error_df = validation_result_df.filter(validation_result_df["validation_result"] == "FAIL") \
                .select(col("detail_id"),col("invoice_number"),col("error_description"),col("field_name_1"),col("field_value_1"),col("field_name_2"),col("field_value_2"),col("validation_no"),col("validation_result")) \
                .cache()
            validation_error_df.printSchema()
            validation_error_df.show(5, truncate=False)
            validation_error_count = validation_error_df.count()
            logger.info(f"Validation Error Bill Record Count => {validation_error_count}")

            validation_success_df = validation_result_df.filter(validation_result_df["validation_result"] == "PASS") \
                .select(col("detail_id"),col("invoice_number"),col("error_description"),col("field_name_1"),col("field_value_1"),col("field_name_2"),col("field_value_2"),col("validation_no"),col("validation_result")).cache()
            validation_success_df.printSchema()
            validation_success_df.show(5, truncate=False)
            validation_success_count = validation_success_df.count()
            logger.info(f"Validation Success Bill Record Count => {validation_success_count}")

            filtered_success_df = validation_success_df.filter((col("detail_id").isNotNull()) & (trim(col("detail_id")) != ""))
            filtered_success_df.printSchema()
            filtered_success_df.show(5, truncate=False)
            filtered_success_count = filtered_success_df.count()
            logger.info(f"Validation Success Bill Record Count => {filtered_success_count}")

            join_condition = filtered_success_df["detail_id"] == validation_error_df["detail_id"]
            validation_success_filter_df = filtered_success_df.join(validation_error_df,join_condition,'leftanti')
            validation_success_filter_df.printSchema()
            validation_success_filter_df.show(5, truncate=False)
            validation_success_filter_count = validation_success_filter_df.count()
            logger.info(f"Final Validation Pass Record Count => {validation_success_filter_count}")

            if validation_error_count > 0:
                logger.info(f"Found Validation Errors. Logging errors to Error Log Table...")
                log_error_table_db="txm_inbound_feeds_logs.batch_inbound_feeds_error_details"
                v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                logger.info(f"v_load_timestamp => {v_load_timestamp}")
                validation_error_log_df = validation_error_df \
                    .withColumn('batch_id',lit(batch_id)) \
                    .withColumn('step_id',lit(step_id)) \
                    .withColumnRenamed('invoice_number','txm_invoice_number') \
                    .withColumn('error_code',lit('Enhanced Validation Error')) \
                    .withColumn('error_info',concat(lit("Validation "),col("validation_no"))) \
                    .withColumnRenamed('error_description','error_message') \
                    .withColumn('create_user',lit('Glue - ETL - Feeds Enhanced')) \
                    .withColumn('update_user',lit('Glue - ETL - Feeds Enhanced')) \
                    .withColumn('create_timestamp',lit(v_load_timestamp)) \
                    .withColumn('update_timestamp',lit(v_load_timestamp)) \
                    .select(col("batch_id"),col("step_id"),col("txm_invoice_number"),col("error_code"),col("error_info"),col("error_message"),col("create_user"),col("update_user"),col("create_timestamp"),col("update_timestamp"))
                validation_error_log_df.printSchema()
                validation_error_log_df.show(5, truncate=False)
                errorLogRecordCount = validation_error_log_df.count()
                logger.info(f"Validation Error log result Record Count => {errorLogRecordCount}")
                logger.info(f'Load validation_error_log_df dataframe to RDS table => {log_error_table_db}')
                lib.load_data_to_rds(validation_error_log_df, log_error_table_db, secrets, mode)

                logger.info(f"Found Validation Errors. Sending Consolidated Error mails with error details with attach...")
                process='Deleted Invoice Validation'
                response = feeds_lib.log_and_email_attachment(validation_error_df,process,env_profile, batch_id, feed_type, provider, s3_bucket, s3_error_attachment_prefix, s3_error_attachment_prefix,input_file_name, error_alerts_to_email_ids, lambda_function_nodemailer)
                logger.info(f"ErrorLog File Created successfully: {response}")
            else:
                logger.info("No Validation Error found in the Batch...")

            if validation_success_filter_count > 0:
                validation_success_filter_df=validation_success_filter_df.select("invoice_number").distinct()
                logger.info(f"Found Validation Success Invoice Number. Updating the status...")
                status = "MARKED_FOR_DELETION"
                status_type = 'ADJUDICATION'
                lib.update_invoice_status(validation_success_filter_df, secrets, status_type, status, feed_type, header_id, "Glue - ETL - Feeds Enhanced")
            
            if is_reprocess_exec:
                status = "COMPLETED_IN_REPROCESS"
                feeds_lib.feeds_step_logging(secrets, failed_batch_id, failed_step_id, job_name, job_id, status, 'Glue - Feeds Inbound Enhanced Load')
            logger.info(f"[SUCCESS] Successfully updated step_id {step_id} to status {status}.")

        else:
            is_no_data_execution = True
            logger.info(f"No Data Avaialble for batch_id {batch_id} and File Name {input_file_name}. Skipping the rest of the flow!!!")  

    else:
        is_skip_execution = True
        logger.info(f"Existing batchi_id {failed_batch_id} found for feed {feed_type} with status as {validation_status} for reprocessing flow. No further processing required, skipping rest of the flow!!!")

    # Determine final status
    if is_no_data_execution:
        status = "NO DATA AVAILABLE"
    elif is_skip_execution:
        status = "SKIP_EXECUTION"
    else:
        status = "COMPLETED"

    feeds_lib.feeds_step_logging(secrets, batch_id, step_id, job_name, job_id, status, 'Glue - Feeds Inbound Enhanced Load')
    logger.info(f"[SUCCESS] Successfully updated step_id {step_id} to status {status}.")

except Exception as e:
    status="FAILED"

    logger.error(f"Error in Feeds Inbound Enhanced Load: {e}")
    error_info = "Failed in Feeds Inbound Enhanced Load"
    error_message = str(e)
    
    feeds_lib.feeds_error_logging(secrets, batch_id, step_id, None, status, job_name, job_id, 'FEEDS INBOUND ENHANCED GLUE FAILED', error_info, error_message, 'Glue - Feeds Inbound Enhanced Load', True)
    logger.info(f"[FAILURE] Logged failure for batch_id {batch_id} and step_id {step_id}")

    response = feeds_lib.log_and_email(env_profile, provider, feed_type, reprocess_flag, batch_id, input_file_name, step_function_info, step_function_execution_id, error_info, error_message, "FATAL", sns_topic_arn, "Feeds Inbound Enhanced Load")
    logger.info(f"Email sent successfully!message_id:{response['MessageId']}")
    raise e

try:
    # Commit the job
    job.commit()
    logger.info("Job committed successfully")
except Exception as e:
    logger.error(f"Error committing the job:{e}")
    raise