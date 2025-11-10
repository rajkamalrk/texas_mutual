import sys
import logging
import json
import base64
import boto3
import pytz
import time
from botocore.exceptions import ClientError
from botocore.client import Config
from awsglue.job import Job
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
import pymysql
from datetime import datetime, date
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import StructType, StringType
from glue_commons import bitx_glue_common as lib
from glue_commons import bitx_feeds_glue_common as feeds_lib

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("claimant-billingestion-feeds-deleted-invoice-load-job")
INBOUND_FEEDS_LOG_SCHEMA = "txm_inbound_feeds_logs"
BATCH_INBOUND_FEEDS_MASTER_TABLE = f"{INBOUND_FEEDS_LOG_SCHEMA}.batch_inbound_feeds_master"
BATCH_INBOUND_FEEDS_STEP_DETAILS_TABLE = f"{INBOUND_FEEDS_LOG_SCHEMA}.batch_inbound_feeds_step_details"
BATCH_INBOUND_FEEDS_ERROR_DETAILS_TABLE = f"{INBOUND_FEEDS_LOG_SCHEMA}.batch_inbound_feeds_error_details"

## @params: [claimant-billingestion-feeds-deleted-invoice-load-job]
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'env_profile',
    'feed_type',
    'provider',
    'batch_id',
    'db_secret_key',
    'sns_topic_arn',
    's3_bucket',
    's3_error_attachment_prefix',
    'error_alerts_to_email_ids',
    'lambda_function_nodemailer',
    'nuxeo_lambda_function',
    'input_file_name',
    'reprocess_flag',
    'failed_batch_id',
    'step_function_info',
    'step_function_execution_id',
    'retries',
    'retry_delay',
    'retry_multiplier'
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

# Extract job parameters
env_profile = args['env_profile']
feed_type = args['feed_type']
provider = args['provider']
batch_id = int(args['batch_id'])
db_secret_key = args['db_secret_key']
sns_topic_arn = args['sns_topic_arn']
s3_bucket = args['s3_bucket']
s3_error_attachment_prefix = args['s3_error_attachment_prefix']
error_alerts_to_email_ids = args['error_alerts_to_email_ids']
lambda_function_nodemailer = args['lambda_function_nodemailer']
nuxeo_lambda_function = args['nuxeo_lambda_function']
input_file_name=args['input_file_name'].strip()
reprocess_flag = args['reprocess_flag']
failed_batch_id = args['failed_batch_id']
step_function_info = args.get('step_function_info')
step_function_execution_id = args.get('step_function_execution_id')
retries = int(args['retries'])
retry_delay = int(args['retry_delay'])
retry_multiplier = int(args['retry_multiplier'])

# Generic methods are now imported from bitx_glue_common library
# Feeds-specific methods are now imported from bitx_feeds_glue_common library   
    
def invoke_nuxeo_delete_invoices(invoice_number, batch_id, step_id, bill_id, retries, retry_delay, retry_multiplier):
    """Invokes the Nuxeo delete invoices Lambda function with retry mechanism"""
    logger.info('Invoking Nuxeo delete invoices lambda function')
    payload = {
        "invoiceNumber": str(invoice_number),
        "batchId": str(batch_id),
        "stepId": str(step_id),
        "billId": str(bill_id)
    }
    logger.info(f"Payload to Lambda: {json.dumps(payload)}")
    delay = retry_delay
    delete_request_response_status = None
    boto_config = Config(connect_timeout=120, read_timeout=300)
    lambda_client = boto3.client('lambda', region_name='us-east-1', config=boto_config)
    
    for attempt in range(0, retries + 1):
        logger.info(f"Attempt: {attempt}")
        try:
            response = lambda_client.invoke(
                FunctionName=nuxeo_lambda_function,
                InvocationType='RequestResponse',
                Payload=json.dumps(payload)
            )
            response_payload = response['Payload'].read().decode('utf-8')
            logger.info(f"Lambda response payload: {response_payload}")
            response_payload = json.loads(response_payload)

            if response_payload and 'statusCode' in response_payload and 'body' in response_payload:
                statusCode = str(response_payload['statusCode']).upper()
                response_body_payload = response_payload['body']
                if isinstance(response_body_payload, str):
                    response_body_payload = json.loads(response_body_payload)
                lambdaMessage = response_body_payload.get('message')
                logger.info(f"invoiceNumber status=>{invoice_number},{statusCode},{lambdaMessage}")

                if statusCode == '200':
                    delete_request_response_status = lambdaMessage if lambdaMessage else 'SUCCESS'
                else:
                    delete_request_response_status = lambdaMessage if lambdaMessage else 'FAILED IN LAMBDA'
                    if not lambdaMessage:
                        raise Exception(f"Lambda response Body not in expected format. {response_payload}")
            else:
                delete_request_response_status = 'FAILED IN LAMBDA'
                raise Exception(f"Lambda response not in expected format. {response_payload}")
            
            # Retry mechanism for specific error codes
            if statusCode in ['502', '503', '504', 'UNKNOWN']:
                logger.info(f'Retry delayed {delay} for Attempt {attempt + 1}...')
                time.sleep(delay)
                delay = retry_delay * retry_multiplier
            else:
                break

        except Exception as e:
            logger.error(f"Error invoking Lambda function: {str(e)}")
            delete_request_response_status = "FAILED IN LAMBDA"
            error_info = f"Error occurred while invoking Nuxeo delete invoices lambda function (Attempt - {attempt})"
            error_message = str(e).replace("'", "''").replace("\\", "\\\\")
            logger.error(f"error_info => {error_info}")  
            logger.error(f"error_message: {error_message}")
            
            # Log error details using the feeds error logging format
            try:
                feeds_lib.feeds_error_logging(secrets, batch_id, step_id, invoice_number, status, job_name, job_id, 'NUXEO_LAMBDA_INVOKE_FAILED', error_info, error_message, 'Glue - Feeds Deleted Invoice Load', False)
            except Exception as log_error:
                logger.error(f"Failed to log error details: {log_error}")
            
            logger.info(f'Retry delayed {delay} for Attempt {attempt + 1}...')
            time.sleep(delay)
            delay = retry_delay * retry_multiplier
    return delete_request_response_status

# Initializations
mode = "append"
status = "IN PROGRESS"
step_id = None
failed_step_id = None
is_nuxeo_error = False
is_no_data_execution = False
error_logging_table = "txm_inbound_feeds_logs.batch_inbound_feeds_error_details"
is_reprocess_exec=False
is_execution=True
is_skip_execution=False
reprocess_valid_list=['COMPLETED','COMPLETED_IN_REPROCESS']

try:
    logger.info(f"Processing STARTED for {feed_type} Deleted Invoice Load...")
    logger.info(f"db_secret_key => {db_secret_key}")
    secrets = lib.get_secret(db_secret_key, 'us-east-1')
    
    # Insert step log and get step_id
    step_id = feeds_lib.feeds_step_logging(secrets, batch_id, step_id, job_name, job_id, 'IN PROGRESS', 'Glue - Feeds Deleted Invoice Load')
    logger.info(f"Step record inserted with step_id => {step_id}")
    
    # Set load timestamp
    v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    logger.info(f"v_load_timestamp => {v_load_timestamp}")

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
        validation_invoice_status=feeds_lib.run_sql_query(validate_sql, secrets)
        print(f"validation_stg_status => {validation_invoice_status}")

        if int(failed_batch_id) > 0:
            failed_batch_id = failed_batch_id
        else:
            failed_batch_id = batch_id

        if not validation_invoice_status:
            failed_step_id = feeds_lib.feeds_step_logging(secrets, failed_batch_id, failed_step_id, job_name, job_id, status, 'Glue - Feeds Inbound Staging Load')
            logger.info(f"Step record inserted with failed_step_id => {failed_step_id}")
            is_reprocess_exec=True

        else:
            file_name=validation_invoice_status[0]['file_name']
            validation_status=validation_invoice_status[0]['status']
            failed_step_id=validation_invoice_status[0]['step_id']
            if feed_type == 'DELETED_INVOICE':
                if isinstance(validation_invoice_status, list):
                    if validation_status in reprocess_valid_list:
                        print(f"Batch=> {failed_batch_id} with file name => {file_name} is already processed succesfully in the last run.Terminating Reprocess!")
                        is_execution=False
                        is_reprocess_exec=False

                    else:
                        logger.info(f"Existing batchi_id {failed_batch_id} found for feed {feed_type} with status as {validation_status}. Starting for reprocessing flow.")
                        is_execution=True
                        is_reprocess_exec=True
                        batch_id=failed_batch_id

    if is_execution:
        # Query to fetch deleted invoice details based on the specified filter criteria
        # This is an independent process that processes all deleted invoices regardless of source/trading partner
        # Additional check: at least one document should be active (is_active = 'Y') for the respective invoice
        read_db_sql = f"""
            SELECT DISTINCT 
                b.bill_id,
                b.txm_invoice_number,
                b.bill_header_id,
                i.status_type,
                i.status
            FROM txm_bitx.bill_header b
            JOIN txm_bitx.invoice_status i ON b.txm_invoice_number = i.txm_invoice_number
            WHERE (
                (i.status_type = 'CLAIM_MATCH_MANUAL' AND i.status = 'MARKED_FOR_DELETION') OR
                (i.status_type = 'VENDOR_MATCH_MANUAL' AND i.status = 'MARKED_FOR_DELETION') OR
                (i.status_type = 'INDICATED_ATTACHMENT_MISSING' AND i.status = 'MARKED_FOR_DELETION') OR
                (i.status_type = 'ADJUDICATION' AND i.status = 'MARKED_FOR_DELETION')
            )
        """

        deleted_invoice_df = lib.get_df_db_data_with_query(spark, read_db_sql, secrets)
        deleted_invoice_df = deleted_invoice_df.cache()
        deleted_invoice_df.printSchema()
        deleted_invoice_df.show(5, truncate=False)
        deleted_invoice_count = deleted_invoice_df.count()
        logger.info(f"Deleted Invoice Record Count => {deleted_invoice_count}")

        if deleted_invoice_count > 0:
            logger.info(f"Processing {deleted_invoice_count} deleted invoices for Nuxeo document deletion...")

            # Register UDF for invoking Nuxeo delete invoices Lambda
            invokeNuxeoDeleteUDF = udf(invoke_nuxeo_delete_invoices, StringType())

            # Invoke Nuxeo delete invoices Lambda for each deleted invoice
            logger.info(f"Invoking Nuxeo delete invoices Lambda for {deleted_invoice_count} invoices...")
            delete_response_df = deleted_invoice_df.withColumn(
                "delete_response", 
                invokeNuxeoDeleteUDF(
                    deleted_invoice_df['txm_invoice_number'],
                    lit(batch_id),
                    lit(step_id),
                    deleted_invoice_df['bill_id'],
                    lit(retries),
                    lit(retry_delay),
                    lit(retry_multiplier)
                )
            )
            delete_response_df = delete_response_df.cache()
            delete_response_df.printSchema()
            delete_response_df.show(5, truncate=False)

            # Group by response status to analyze results
            delete_response_df.groupBy('delete_response').count().show(truncate=False)

            # Check for any errors in the error table for this batch and step
            error_table_sql = f"""
                SELECT 
                    COALESCE(err.txm_invoice_number, '') as txm_invoice_number,
                    err.error_code,
                    err.error_info,
                    CONCAT(err.error_code, ' - ', IFNULL(REPLACE(CONVERT(err.error_message USING utf8), '"', ''''''), '')) AS error_message
                FROM {error_logging_table} err
                WHERE err.batch_id = '{batch_id}' AND err.step_id = {step_id}
            """
            
            error_table_df = lib.get_df_db_data_with_query(spark, error_table_sql, secrets)
            error_count = error_table_df.count()
            logger.info(f"Retrieved {error_count} error records from error table for batch_id={batch_id}, step_id={step_id}")
            
            if error_count > 0:
                is_nuxeo_error = True
                logger.info(f"Found errors in error table. Sending Consolidated Error Email!!!")
                
                # Prepare error data for consolidated email from error table
                error_df = error_table_df.filter(
                    col("txm_invoice_number") != ""
                ).select(
                    col("txm_invoice_number").alias("invoice_number"),
                    lit("").alias("bill_id"),  # bill_id not available in error table
                    lit("Error Table Error").alias("error_description"),
                    lit("Error Code").alias("field_name_1"),
                    col("error_code").alias("field_value_1"),
                    col("error_info").alias("field_name_2"),
                    col("error_message").alias("field_value_2")
                )
                
                # Send consolidated error email
                process = 'Deleted Invoice Nuxeo Document Deletion'
                response = feeds_lib.log_and_email_attachment(error_df, process, env_profile, batch_id, feed_type, provider, s3_bucket, s3_error_attachment_prefix, s3_error_attachment_prefix, input_file_name, error_alerts_to_email_ids, lambda_function_nodemailer)
                logger.info(f"Error Email sent successfully: {response}")
            else:
                logger.info(f"No errors found in error table for this batch and step!!!")

            # Update status to completed
            status = "COMPLETED"
            logger.info(f"Deleted Invoice Load completed successfully for {feed_type}")
            if is_reprocess_exec:
                status = "COMPLETED_IN_REPROCESS"
                feeds_lib.feeds_step_logging(secrets, failed_batch_id, failed_step_id, job_name, job_id, status, 'Glue - Feeds Deleted Invoice Load')
                logger.info(f"[SUCCESS] Successfully updated step_id {failed_step_id} to status {status}.")

        else:
            logger.info(f"No Data Available for batch_id {batch_id}. Skipping the rest of the flow!!!")
            is_no_data_execution = True
            # Update status for no data scenario
            status = "NO DATA AVAILABLE"

    else:
        logger.info(f"Existing batchi_id {failed_batch_id} found for feed {feed_type} with status as {validation_status} for reprocessing flow. No further processing required, skipping rest of the flow!!!")
        is_skip_execution=True

    # Determine final status
    if is_nuxeo_error:
        status = "COMPLETED WITH NUXEO ERRORS"
    elif is_no_data_execution:
        status = "NO DATA AVAILABLE"
    elif is_skip_execution:
        status = "SKIP_EXECUTION"
    else:
        status = "COMPLETED"

    feeds_lib.feeds_step_logging(secrets, batch_id, step_id, job_name, job_id, status, 'Glue - Feeds Deleted Invoice Load')
    logger.info(f"[SUCCESS] Successfully updated step_id {step_id} to status {status}.")
    
    logger.info(f"Processing COMPLETED for {feed_type} Deleted Invoice Load!!!")
    
except Exception as e:
    status = "FAILED"
    logger.error(f"Error in Deleted Invoice Load: {e}")
    
    error_info = "Failed in Deleted Invoice Load"
    error_message = str(e)
    
    feeds_lib.feeds_error_logging(secrets, batch_id, step_id, None, status, job_name, job_id, 'FEEDS DELETED INVOICE GLUE FAILED', error_info, error_message, 'Glue - Feeds Deleted Invoice Load', True)
    logger.info(f"[FAILURE] Logged failure for batch_id {batch_id} and step_id {step_id}")

    response = feeds_lib.log_and_email(env_profile, provider, feed_type, reprocess_flag, batch_id, input_file_name, step_function_info, step_function_execution_id, error_info, error_message, "FATAL", sns_topic_arn, "Deleted Invoice Load")
    logger.info(f"Email sent successfully!message_id:{response['MessageId']}")

    raise e
finally:
    if reprocess_flag.strip().upper()=='Y' and int(failed_batch_id) == batch_id and (status.upper() in ('COMPLETED','NO DATA AVAILABLE','COMPLETED WITH NUXEO ERRORS')):
        status='COMPLETED_IN_REPROCESS'
        feeds_lib.update_feeds_batch_status(secrets, failed_batch_id, status, 'Glue - Feeds Deleted Invoice Load')
        print(f"[SUCCESS] Successfully updated batch_status_id {batch_id} to status {status}.")

try:
    # Commit the job
    job.commit()
    logger.info("Job committed successfully")
except Exception as e:
    logger.error(f"Error committing the job:{e}")
    raise