import sys
import logging
import boto3
import pymysql
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit, col, lower
from glue_commons import bitx_glue_common as lib
from glue_commons import bitx_feeds_glue_common as feeds_lib


#Configure logging.
logging.basicConfig(level=logging.INFO)
#set the logging level to INFO
logger = logging.getLogger("claimant-billingestion-feeds-inbound-staging-load-job")

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    's3_bucket',
    's3_input_files_prefix',
    'datacatalog_schema_name',
    'datacatalog_config_table_name',
    'db_secret_key',
    'batch_id',
    'env_profile',
    'sns_topic_arn',
    'input_file_name',
    'feed_type',
    'provider',
    'failed_batch_id',
    'reprocess_flag',
    'step_function_info',
    'step_function_execution_id',
    's3_archive_prefix',
    's3_error_prefix'
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
db_secret_key=args['db_secret_key'].strip()
batch_id=args['batch_id'].strip()
env_profile=args['env_profile'].strip()
sns_topic_arn=args['sns_topic_arn'].strip()
s3_input_files_prefix=args['s3_input_files_prefix'].strip()
input_file_name=args['input_file_name'].strip()
feed_type=args['feed_type']
provider = args['provider']
failed_batch_id = args['failed_batch_id']
reprocess_flag = args['reprocess_flag']
step_function_info = args.get('step_function_info')
step_function_execution_id = args.get('step_function_execution_id')
s3_archive_prefix=args['s3_archive_prefix']
s3_error_prefix=args['s3_error_prefix']

#intitilizations
mode = "append"
status="IN PROGRESS"
step_id = None
failed_step_id = None
s3_path=f"s3://{s3_bucket}/{s3_input_files_prefix}{input_file_name}"
logger.info(f"s3_path => {s3_path}")
archive_key = ""
is_reprocess_exec=False
is_execution=True
is_no_data_execution=False
is_skip_execution=False
reprocess_valid_list=['COMPLETED','COMPLETED_IN_REPROCESS']

#Replacing FeedType, Provider in archive and error location
logger.info(f"s3_archive_prefix => {s3_archive_prefix}")
s3_archive_prefix = s3_archive_prefix.replace("<PROVIDER>",provider.lower()).replace("<FEED_TYPE>",feed_type.lower())
logger.info(f"NEW s3_archive_prefix => {s3_archive_prefix}")

logger.info(f"s3_error_prefix => {s3_error_prefix}")
s3_error_prefix = s3_error_prefix.replace("<PROVIDER>",provider.lower()).replace("<FEED_TYPE>",feed_type.lower())
logger.info(f"NEW s3_error_prefix => {s3_error_prefix}")

# Generic methods are now imported from bitx_glue_common library
# Feeds-specific methods are now imported from bitx_feeds_glue_common library 
        
try:
    logger.info(f"Processing STARTED for {feed_type} Staging Load...")

    logger.info(f"db_secret_key => {db_secret_key}")
    secrets = lib.get_secret(db_secret_key, 'us-east-1')

    # Insert step log and get step_id
    step_id = feeds_lib.feeds_step_logging(secrets, batch_id, step_id, job_name, job_id, status, 'Glue - Feeds Inbound Staging Load')
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
                     WHERE m.batch_id={failed_batch_id} AND st.job_name='{job_name}'"""
        validation_stg_status=feeds_lib.run_sql_query(validate_sql, secrets)
        print(f"validation_stg_status => {validation_stg_status}")

        if not validation_stg_status:
            failed_step_id = feeds_lib.feeds_step_logging(secrets, failed_batch_id, failed_step_id, job_name, job_id, status, 'Glue - Feeds Inbound Staging Load')
            logger.info(f"Step record inserted with failed_step_id => {failed_step_id}")
            is_reprocess_exec=True

        else:
            file_name=validation_stg_status[0]['file_name']
            validation_status=validation_stg_status[0]['status']
            failed_step_id=validation_stg_status[0]['step_id']
            if feed_type == 'DELETED_INVOICE':
                if isinstance(validation_stg_status, list):
                    if validation_status in reprocess_valid_list:
                        print(f"Batch=> {failed_batch_id} with file name => {file_name} is already processed succesfully in the last run.Terminating Reprocess!")
                        is_execution=False
                        is_reprocess_exec=False
                        status = "SKIP_EXECUTION"

                    else:
                        del_failed_batch_data=f"""DELETE FROM txm_deleted_invoice_staging.delete_file_header  where batch_id={failed_batch_id}"""
                        feeds_lib.run_sql_query(del_failed_batch_data, secrets)
                        logger.info(f"Moving file from error folder to source folder for re-processing...")
                        error_prefix=f"{s3_error_prefix}{file_name}"
                        destination_prefix=f"{s3_input_files_prefix}{file_name}"
                        s3_client = boto3.client("s3")
                        response=feeds_lib.move_s3_file(destination_prefix, s3_bucket, error_prefix, s3_client)
                        logger.info(f"File Moved from to Error Folder=>{response}")
                        is_execution=True
                        is_reprocess_exec=True

    if is_execution:
        #Reading data file from S3
        logger.info(f"Reading {feed_type} data file from S3...")
        init_df=spark.read.text(s3_path).toDF("delete_invoice_data_str")
        init_df.printSchema()
        init_df.show(5, truncate=False)
        file_record_count = init_df.count()
        logger.info(f"Data File Record Count => {file_record_count}")
        init_itr=init_df.rdd.toLocalIterator()

        if file_record_count > 0:
            logger.info(f"Reading Bill Ingestion configuration from Data Catalog table => {datacatalog_schema_name}.{datacatalog_config_table_name}...")
            AWSGlueDataCatalog_StagingConfig = glueContext.create_dynamic_frame.from_catalog(database=datacatalog_schema_name, table_name=datacatalog_config_table_name, transformation_ctx="AWSGlueDataCatalog_StagingConfig")
            AWSGlueDataCatalog_StagingConfig.printSchema()
            AWSGlueDataCatalog_StagingConfig.show()
            config_df = AWSGlueDataCatalog_StagingConfig.toDF()
            config_rec = config_df.filter(lower(col("feed_type")) == feed_type.lower()).first()
            logger.info(f"config_rec => {config_rec}")

            logger.info(f'Reading {feed_type} configuration from Data Catalog table => {config_rec["datacatalog_schema_name"]}.{config_rec["datacatalog_table_name"]}...')
            AWSGlueDataCatalog_StagingTPConfig = glueContext.create_dynamic_frame.from_catalog(database=config_rec["datacatalog_schema_name"], table_name=config_rec["datacatalog_table_name"], transformation_ctx="AWSGlueDataCatalog_StagingTPConfig")
            AWSGlueDataCatalog_StagingTPConfig.printSchema()
            AWSGlueDataCatalog_StagingTPConfig.show()
            feed_type_config_df = AWSGlueDataCatalog_StagingTPConfig.toDF()
            feed_type_config_df.show(3, truncate=False)
            recordCount = feed_type_config_df.count()
            logger.info(f"{feed_type} config Record Count => {recordCount}")
            feed_type_config_itr = feed_type_config_df.orderBy(['order_seq_no']).rdd.toLocalIterator()

            for record in feed_type_config_itr:
                logger.info(f"Processing started for row: {record}")
                v_load_timestamp = None
                v_filter_id_col_value = None
                #Filtering each table data based on first two characters and loaded into a dataframe
                record_type_data_df = init_df.filter(col("delete_invoice_data_str").substr(1, len(record["record_type_code"])) == record["record_type_code"])
                
                record_type_data_df.createOrReplaceTempView(record['df_table_name'])
                feeds_inbound_df=spark.sql(record['transform_sql_select'])
                feeds_inbound_df.printSchema()
                feeds_inbound_df.show(5, truncate=False)
                record_count=feeds_inbound_df.count()
                logger.info(f"record count=>{record_count}")

                v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                logger.info(f"v_load_timestamp => {v_load_timestamp}")

                feeds_inbound_df = feeds_inbound_df \
                .withColumn("create_user", lit("Glue ETL-Feed Type Staging load")) \
                .withColumn("update_user", lit("Glue ETL-Feed Type Staging load")) \
                .withColumn("create_timestamp", lit(v_load_timestamp)) \
                .withColumn("update_timestamp", lit(v_load_timestamp))

                if is_reprocess_exec:
                    input_batch_id = failed_batch_id
                else:
                    input_batch_id = batch_id

                if record["load_type"] in ("HEADER"):
                    feeds_inbound_df = feeds_inbound_df.withColumn("batch_id", lit(input_batch_id)) \
                    .withColumn("file_name", lit(input_file_name))

                feeds_inbound_df.printSchema()
                feeds_inbound_df.show(1, truncate=False)
                recordCount = feeds_inbound_df.count()
                logger.info(f"Record Type Data Transform Record Count => {recordCount}")

                logger.info(f'Load transformed dataframe to RDS table => {record["db_table_name"]}')
                lib.load_data_to_rds(feeds_inbound_df, record["db_table_name"], secrets, mode)

                logger.info(f'load_type => {record["load_type"]}')
                if record["load_type"] in ("HEADER"):
                    if recordCount > 0:
                        logger.info(f"Record Type is File Header - extracting generated id(s)...")
                        logger.info(f'db_filter_id_col_name => {record["db_filter_id_col_name"]}')
                        v_filter_id_col_value = feeds_inbound_df.first()[record["db_filter_id_col_name"]]
                        logger.info(f"v_filter_id_col_value => {v_filter_id_col_value}")
                        if dict(feeds_inbound_df.dtypes)[record["db_filter_id_col_name"]] not in ["int","bigint"]:
                            v_filter_id_col_value = f"'{v_filter_id_col_value}'"
                            logger.info(f"v_filter_id_col_value => {v_filter_id_col_value}")
                        if record["load_type"] != "HEADER":
                            v_load_timestamp = None
                        logger.info(f"v_load_timestamp => {v_load_timestamp}")
                        db_filter_id_df = lib.get_df_db_data_with_filter_id(spark, record["db_table_name"], record["db_select_cols_list"], record["db_filter_id_col_name"], v_filter_id_col_value, v_load_timestamp, secrets)
                        db_filter_id_df = db_filter_id_df.cache()
                        db_filter_id_df.createOrReplaceTempView("db_" + record["df_table_name"])
                        db_filter_id_df.show(5, truncate=False)
                        recordCount = db_filter_id_df.count()
                        logger.info(f"DB Filter IDs Record Count => {recordCount}")
                        if record["load_type"] == "HEADER":
                            if "header_id" in db_filter_id_df.columns:
                                header_id = db_filter_id_df.first()["header_id"]
                                logger.info(f'header_id => {header_id}')
                            else:
                                logger.info(f'header_id NOT exists in FILE HEADER table!!!')   
                    else:
                        logger.info(f'No Header Record found for the batch_id = {input_batch_id}. Stoping batch!!!') 
                        
                        error_info = "Failed in Feeds Inbound Staging Load"
                        error_message = "Header Record Not Found"

                        feeds_lib.feeds_error_logging(secrets, input_batch_id, step_id, None, status, job_name, job_id, 'FEEDS INBOUND STAGING GLUE FAILED', error_info, error_message, 'Glue - Feeds Inbound Staging Load', True)
                        logger.info(f"[FAILURE] Logged failure for batch_id {input_batch_id} and step_id {step_id}")

                        response = feeds_lib.log_and_email(env_profile, provider, feed_type, reprocess_flag, input_batch_id, input_file_name, step_function_info, step_function_execution_id, error_info, error_message, "FATAL", sns_topic_arn, "Feeds Inbound Staging Load")
                        logger.info(f"Email sent successfully!message_id:{response['MessageId']}")
                        raise

            #Archiving on successfull process   
            archive_key=f"{s3_archive_prefix}{input_file_name}"
            if is_reprocess_exec:
                status = "COMPLETED_IN_REPROCESS"
                feeds_lib.feeds_step_logging(secrets, failed_batch_id, failed_step_id, job_name, job_id, status, 'Glue - Feeds Inbound Staging Load')

        else:
            is_no_data_execution = True
            logger.info(f"No Data Available for batch_id {batch_id} and File Name {input_file_name}. Skipping the rest of the flow!!!")

    else:
        logger.info(f"Existing batchi_id {failed_batch_id} found for feed {feed_type} with status as {validation_status} for reprocessing flow. No further processing required, skipping rest of the flow!!!")
        is_skip_execution=True

    # Determine final status
    if is_no_data_execution:
        status = "NO DATA AVAILABLE"
    elif is_skip_execution:
        status = "SKIP_EXECUTION"
    else:
        status = "COMPLETED"

    feeds_lib.feeds_step_logging(secrets, batch_id, step_id, job_name, job_id, status, 'Glue - Feeds Inbound Staging Load')
    logger.info(f"[SUCCESS] Successfully updated step_id {step_id} to status {status}.")
    logger.info(f"Processing COMPLETED for {feed_type} feeds inbound staging load!!!")

except Exception as e:
    status="FAILED"

    logger.error(f"Error in Feeds Inbound Staging Load: {e}")
    error_info = "Failed in Feeds Inbound Staging Load"
    error_message = str(e)

    archive_key=f"{s3_error_prefix}{input_file_name}"
    
    feeds_lib.feeds_error_logging(secrets, batch_id, step_id, None, status, job_name, job_id, 'FEEDS INBOUND STAGING GLUE FAILED', error_info, error_message, 'Glue - Feeds Inbound Staging Load', True)
    logger.info(f"[FAILURE] Logged failure for batch_id {batch_id} and step_id {step_id}")

    response = feeds_lib.log_and_email(env_profile, provider, feed_type, reprocess_flag, batch_id, input_file_name, step_function_info, step_function_execution_id, error_info, error_message, "FATAL", sns_topic_arn, "Feeds Inbound Staging Load")
    logger.info(f"Email sent successfully!message_id:{response['MessageId']}")
    raise e
finally:
    logger.info("final  block")   
    if is_execution:
        logger.info("Calling archive function...")
        logger.info(f"destination path => {archive_key}")
        source_key = f"{s3_input_files_prefix}{input_file_name}"
        logger.info(f"source path => {source_key}")
        lib.archive_file(bucketName=s3_bucket,archivekey=archive_key,sourceKey=source_key)
        logger.info("Completed archive!!!")

try:
    # Commit the job
    job.commit()
    logger.info("Job committed successfully")
except Exception as e:
    logger.error(f"Error committing the job:{e}")
    raise