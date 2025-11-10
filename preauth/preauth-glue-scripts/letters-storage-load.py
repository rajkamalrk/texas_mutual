import sys
import logging
import json
import boto3
import pytz
import time
from datetime import datetime
from botocore.exceptions import ClientError
from botocore.client import Config
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.window import Window
from pyspark.sql.functions import lit, col, lower, udf, when, isnan, isnull
from preauth_glue_commons import preauth_glue_common as preauth_lib
from pyspark.sql.types import StructType, StringType

#Configure logging.
logging.basicConfig(level=logging.INFO)
#set the logging level to INFO
logger = logging.getLogger("claimant-billingestion-pre-auth-enhanced-load-job")

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, [ 'JOB_NAME',
    's3_bucket',
    's3_input_files_prefix',
    'db_secret_key',
    'batch_id',
    'env_profile',
    'sns_topic_arn',
    'provider',
    'load_category',
    's3_error_attachment_prefix',
    'error_alerts_to_email_ids',
    'lambda_function_nodemailer',
    'retries',
    'retry_delay',
    'retry_multiplier',
    'nuxeo_lambda_function',
    'reprocess_flag',
    'step_function_info',
    'step_function_execution_id',
    'failed_batch_id'
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
db_secret_key=args['db_secret_key'].strip()
batch_id=args['batch_id'].strip()
env_profile=args['env_profile'].strip()
sns_topic_arn=args['sns_topic_arn'].strip()
s3_input_files_prefix=args['s3_input_files_prefix'].strip()
provider = args['provider'].strip()
load_category = args['load_category'].strip()
s3_error_attachment_prefix=args['s3_error_attachment_prefix'].strip()
error_alerts_to_email_ids=args['error_alerts_to_email_ids'].strip()
lambda_function_nodemailer=args['lambda_function_nodemailer'].strip()
retries=int(args['retries'])
retry_delay=int(args['retry_delay'])
retry_multiplier=int(args['retry_multiplier'])
nuxeo_lambda_function=args['nuxeo_lambda_function']
reprocess_flag = args['reprocess_flag'].strip()
failed_batch_id = args['failed_batch_id'].strip()
step_function_info = args.get('step_function_info')
step_function_execution_id = args.get('step_function_execution_id')
datetime_received=None
mode = "append"
status="IN PROGRESS"
logging_table="txm_preauth_logs.batch_preauth_step_details"
error_logging_table="txm_preauth_logs.batch_preauth_error_details"
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
file_name = s3_input_files_prefix.strip('/').split('/')[-1]
logger.info(f"file name {file_name}...")
source = None

def log_and_email_attachment(failed_bills_log_df,process,env_profile, batch_id, source, provider, bucket_name, source_prefix, target_prefix, file_name):
    logger.info("inside log email attachment method")
    try:
        current_date = datetime.now().strftime('%Y-%m-%d')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        cst_time = datetime.now(pytz.timezone('America/Chicago')).strftime('%Y/%m/%d %H:%M:%SCST')
        source_prefix_with_date = f"{source_prefix}/{current_date}/"
        target_prefix_with_date = f"{target_prefix}/{current_date}/"
        logger.info(f'source_prefix_with_date : {source_prefix_with_date}')
        logger.info(f'target_prefix_with_date : {target_prefix_with_date}')

        failed_bills_error_log_df = failed_bills_log_df.withColumn('Process',lit(process)) \
            .withColumnRenamed('datetime_received','Receive Date') \
            .withColumnRenamed('claim_number','Claim Number') \
            .withColumnRenamed('error_description','Error Description') \
            .withColumnRenamed('field_name_1','Field name 1') \
            .withColumnRenamed('field_value_1','Field Value 1') \
            .withColumnRenamed('field_name_2','Field Name 2') \
            .withColumnRenamed('field_value_2','Field Value 2') \
            .withColumnRenamed('update_timestamp','TimeStamp') \
            .select(col('Receive Date'),col('Claim Number'),col('Error Description'),col('Field Name 1'),col('Field Value 1'),col('Field Name 2'),col('Field Value 2'),col('TimeStamp'))

        failed_bills_error_log_df.printSchema()
        failed_bills_error_log_df.show(10, truncate=False)
        recordCount = failed_bills_error_log_df.count()
        logger.info(f"Validation Error Mail Record Count => {recordCount}")

        failed_bills_error_log_df.coalesce(1).write.option('header','true').mode('append').format('csv').save(f"s3://{bucket_name}/{source_prefix_with_date}")
        if provider and provider.strip() and provider.lower() != "null":
            source_tp_str = provider
            provider=provider.upper()
        else:
            source_tp_str = source
        source_tp_str = source_tp_str.upper()

        s3_client = boto3.client('s3','us-east-1')
        target_file_name = f'Error_log_{env_profile}_{provider}_{batch_id}_{timestamp}.csv'
        logger.info(f'TARGET FILE NAME : {target_file_name}')
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=source_prefix_with_date)

        if 'Contents' in response and response['Contents']:
            files = response['Contents']
            files_sorted = sorted(files, key=lambda x: x['LastModified'], reverse=True)
            source_file = files_sorted[0]["Key"]
            logger.info(f'SOURCE_FILE : {source_file}')

            if source_file:
                target_file = f'{target_prefix_with_date}{target_file_name}'
                copy_source = {'Bucket': bucket_name, 'Key': source_file}
                s3_client.copy_object(CopySource=copy_source, Bucket=bucket_name, Key=target_file)
                s3_client.delete_object(Bucket=bucket_name, Key=source_file)
                logger.info(f"Renamed File {source_file} to {target_file}")

                # Invoke the Lambda function to send an email with the attachment
                client = boto3.client('lambda',region_name='us-east-1')
                payload = {
                    's3Bucket': bucket_name,
                    's3Key': target_file,
                    'to': error_alerts_to_email_ids,
                    'subject': f'{env_profile} - {source_tp_str} - {process}',
                    'html': f"""<p>Please find the attached error log for {source_tp_str} with batch ID {batch_id}.</p>
                                <p>Trading partner: {provider}</p>
                                <p>Batch ID: {batch_id}</p>
                                <p>File Name:{file_name}</p>"""
                }

                try:
                    response = client.invoke(
                        FunctionName=lambda_function_nodemailer,  # nodemailer (nodejs lambda)
                        InvocationType='RequestResponse',
                        Payload=json.dumps(payload)
                    )

                    response_payload = json.loads(response['Payload'].read())
                    logger.info(f'Lambda response: {response_payload}')

                except ClientError as e:
                    logger.error(f"Error invoking Lambda function: {e}")
                    raise

        else:
            logger.info("No objects found in the specified S3 path.")

    except Exception as e:
        logger.error(f"Error: {e}")
        raise 

def invoke_nuxeo(claim_number, header_id, batch_id, step_id, retries, retry_delay, retry_multiplier):
    print('Invoke nuxeo lambda function')
    payload = {
            "claimNumber": claim_number,
            "headerId": header_id,
            "batchId": batch_id,
            "stepId": step_id
        }
    print(f"Payload to Lambda: {json.dumps(payload)}")
    delay = retry_delay
    storage_request_response_status=None
    boto_config = Config(connect_timeout=120, read_timeout=300)
    lambda_client = boto3.client('lambda',region_name='us-east-1',config=boto_config)
    
    for attempt in range(0,retries + 1):
        print(f"Attemp: {attempt}")
        try:
            response = lambda_client.invoke(
                FunctionName=nuxeo_lambda_function,
                InvocationType='RequestResponse',
                Payload=json.dumps(payload)
            )
            response_payload = response['Payload'].read().decode('utf-8')
            print(f"Lambda response payload: {response_payload}")
            response_payload = json.loads(response_payload)

            if response_payload and 'statusCode' in response_payload and 'body' in response_payload:
                statusCode = str(response_payload['statusCode']).upper()
                response_body_payload = response_payload['body']
                if isinstance(response_body_payload, str):
                    response_body_payload = json.loads(response_body_payload)
                lambdaMessage = response_body_payload.get('message')
                print(f"billId status=>{header_id},{statusCode},{lambdaMessage}")

                if statusCode=='200':
                    if lambdaMessage == 'Attachment stored and Nuxeo updated successfully':
                        storage_request_response_status = 'SUCCESS'
                    else:
                        storage_request_response_status= lambdaMessage
                else:
                    if lambdaMessage:
                        storage_request_response_status = lambdaMessage
                    else:
                        storage_request_response_status='FAILED IN LAMBDA'
                        raise Exception(f"Lambda response Body not in expected format. {response_payload}")
            else:
                storage_request_response_status='FAILED IN LAMBDA'
                raise Exception(f"Lambda response not in expected format. {response_payload}")
            
            # When API call fails and list of error codes retry mechanism
            if statusCode in ['502','503','504','UNKNOWN']:
                print(f'Retry delayed {delay} for Attempt {attempt + 1}...')
                time.sleep(delay)
                delay = retry_delay * retry_multiplier
            else:
                break

        except Exception as e:
            print(f"Error invoking Lambda function: {str(e)}")
            storage_request_response_status="FAILED IN LAMBDA"
            errorInfo=f"Error occured while invoking NUXEO lambda function (Attempt - {attempt})"
            errorMessage=str(e).replace("'","''").replace("\\","\\\\")
            print(f"errorInfo => {errorInfo}")  
            print(f"Error: {errorMessage}")

            # Log error details using the preauth error logging format
            try:
                preauth_lib.preauth_error_logging(secrets, batch_id, step_id, claim_number, status, 'NUXEO_LAMBDA_INVOKE_FAILED', errorInfo, errorMessage, 'Glue - PreAuth Letters Storage Load', False)
            except Exception as log_error:
                logger.error(f"Failed to log error details: {log_error}")

            print(f'Retry delayed {delay} for Attempt {attempt + 1}...')
            time.sleep(delay)
            delay = retry_delay * retry_multiplier
    return storage_request_response_status

is_no_data_execution = False

try:
    logger.info(f"Processing STARTED for Pre Auth Letters Storage Load...")
    logger.info(f"db_secret_key => {db_secret_key}")
    secrets = preauth_lib.get_secret(db_secret_key, 'us-east-1')
    step_id = preauth_lib.preauth_step_logging(secrets, batch_id, None, job_name, job_id, status, 'Glue-ETL-Pre-Auth Letters Storage Load')
    logger.info(f"Step record inserted with step_id => {step_id}")
    v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    logger.info(f"v_load_timestamp => {v_load_timestamp}")

    read_db_sql = f"""
        SELECT t1.batch_id, t1.header_id, t1.claim_number, t1.document_name, t1.document_serial_number, t1.file_receive_date AS datetime_received,
        'preauth_letters_data.document_name' AS field_name_1,t1.document_name AS field_value_1,'' AS field_name_2,'' AS field_value_2, '{v_load_timestamp}' AS update_timestamp FROM txm_preauth.preauth_letters_data t1
        LEFT JOIN txm_preauth.preauth_letters_data t2 ON t1.document_name = t2.document_name 
        AND t2.document_serial_number IS NOT NULL 
        WHERE t2.document_name IS NULL AND t1.document_name IS NOT NULL """

    record_list_df = preauth_lib.get_df_db_data_with_query(spark, read_db_sql, secrets)
    record_list_df = record_list_df.cache()
    record_list_df.printSchema()
    record_list_df.show(5, truncate=False)
    record_list_count = record_list_df.count()
    logger.info(f"Record Count => {record_list_count}")
    if record_list_df.count() > 0:
        datetime_received = record_list_df.select("datetime_received").first()["datetime_received"]
    logger.info(f"datetime_received => {datetime_received}")

    if record_list_count > 0:
        logger.info(f"Data Available for Batch ID {batch_id}...")

        v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        logger.info(f"v_load_timestamp => {v_load_timestamp}")
        nuxeo_df=record_list_df.select("claim_number", "header_id").distinct()
        nuxeo_df=nuxeo_df.cache()
        nuxeo_df.printSchema()
        nuxeo_df.show(5, truncate=False)
        nuxeo_df_count = nuxeo_df.count()
        logger.info(f"Nuxeo invoke Record Count => {nuxeo_df_count}")

        invokeNuxeoUDF = udf(invoke_nuxeo,StringType())

        logger.info('Invoking Nuxeo Lambda...')
        letters_nuxeo_response_df = nuxeo_df.withColumn("nuxeo_response",invokeNuxeoUDF(col("claim_number"),col("header_id"),lit(batch_id),lit(step_id),lit(retries),lit(retry_delay),lit(retry_multiplier))) 
        letters_nuxeo_response_df = letters_nuxeo_response_df.cache()
        letters_nuxeo_response_df.printSchema()
        letters_nuxeo_response_df.show(10, truncate=False)
        letters_nuxeo_response_count = letters_nuxeo_response_df.count()
        logger.info(f"Nuxeo API Response Record Count => {letters_nuxeo_response_count}")

        df_meta = letters_nuxeo_response_df.alias("meta")
        df_valid = record_list_df.alias("valid")

        join_condition = [df_meta['header_id'] == df_valid['header_id']]
        final_letters_df = df_meta.join(df_valid,join_condition,"inner")
        final_letters_df = final_letters_df.select(col("batch_id"),col("meta.header_id"),col("datetime_received"),col("meta.claim_number"),col("nuxeo_response"),col("field_name_1"),col("field_value_1"),col("field_name_2"), col("field_value_2"), col("update_timestamp"))
        final_letters_df=final_letters_df.cache()
        final_letters_df.printSchema()
        final_letters_df.show(5, truncate=False)
        final_letters_count = final_letters_df.count()
        logger.info(f"Claim matches for batch Record Count => {final_letters_count}")

        errors_df = final_letters_df.filter(final_letters_df["nuxeo_response"] != "SUCCESS")
        errors_df.printSchema()
        errors_df.show(5, truncate=False)
        errors_count = errors_df.count()
        logger.info(f"Final Fail Validation Count => {errors_count}")

        if errors_count > 0:
            #email part
            logger.info(f"Found detail_ids with Failed Validations...")
            
            # Fetch error details from error table for the failed claims
            error_details_sql = f"""
                SELECT 
                    COALESCE(err.claim_number, '') as claim_number,
                    err.error_code,
                    err.error_info,
                    CONCAT(err.error_code, ' - ', IFNULL(REPLACE(CONVERT(err.error_message USING utf8), '"', ''''''), '')) AS error_description
                FROM {error_logging_table} err
                WHERE err.batch_id = '{batch_id}' AND err.step_id = {step_id}
            """
            
            error_details_df = preauth_lib.get_df_db_data_with_query(spark, error_details_sql, secrets)
            error_details_df = error_details_df.cache()
            error_details_df.printSchema()
            error_details_df.show(5, truncate=False)
            error_details_count = error_details_df.count()
            logger.info(f"Retrieved {error_details_count} error detail records from error table")
            
            # Join the errors_df with error_details_df to get proper error descriptions
            if error_details_count > 0:
                # Create aliases for the DataFrames
                errors_alias = errors_df.alias("errors")
                error_details_alias = error_details_df.alias("error_details")
                
                # Join on claim_number to get error descriptions from error table
                joined_errors_df = errors_alias.join(
                    error_details_alias, 
                    errors_alias["claim_number"] == error_details_alias["claim_number"], 
                    "left"
                )
                
                # Select columns and use error_description from error table, fallback to nuxeo_response if not found
                failed_bills_log_df = joined_errors_df.select(
                    col("errors.datetime_received"),
                    col("errors.claim_number"),
                    col("errors.field_name_1"),
                    col("errors.field_value_1"),
                    col("errors.field_name_2"),
                    col("errors.field_value_2"),
                    col("errors.update_timestamp"),
                    # Use error_description from error table if available, otherwise use nuxeo_response
                    when(col("error_details.error_description").isNull(), col("errors.nuxeo_response"))
                    .otherwise(col("error_details.error_description")).alias("error_description")
                ).distinct()
            else:
                # Fallback to original logic if no error details found in error table
                logger.info("No error details found in error table, using nuxeo_response as error_description")
                failed_bills_log_df = errors_df \
                    .withColumnRenamed('nuxeo_response','error_description') \
                    .select(col("datetime_received"),col("claim_number"),col("error_description"),col("field_name_1"),col("field_value_1"),col("field_name_2"), col("field_value_2"), col("update_timestamp"))
            
            failed_bills_log_df.printSchema()
            failed_bills_log_df.show(5, truncate=False)
            recordCount = failed_bills_log_df.count()
            logger.info(f"Failed Bills Logs DF result Record Count => {recordCount}")

            process = 'Preauth Letters Storage Load - Error Table Errors'
            if failed_bills_log_df.count() > 0 :
                if reprocess_flag == 'Y':
                    file_name = None
                response = log_and_email_attachment(failed_bills_log_df, process, env_profile, batch_id, source, provider, s3_bucket, s3_error_attachment_prefix, s3_error_attachment_prefix, file_name)
                logger.info(f"Error Email sent successfully: {response}")
        else:
            logger.info(f"No failure Letters Avaialble For Batch ID {batch_id}...")
    else:
        is_no_data_execution = True
        logger.info(f"No Data Avaialble Batch ID {batch_id}. Skipping the rest of the flow!!!")
        preauth_lib.preauth_step_logging(secrets, batch_id, step_id, None, None, 'COMPLETED', 'Glue-ETL-Pre-Auth Letters Storage Load')
        logger.info(f"[SUCCESS] Successfully updated step_id {step_id} to status {status}.")

    #Data for log table
    if is_no_data_execution:
        status="FAILED_NO_DATA"
        #Error Flow
        error_info = "No Data Found"
        error_message = f"CustomError: Data NOT Available in Enhanced for Batch ID {batch_id}."
        logger.error(f"error_message => {error_message}")
        preauth_lib.log_and_email(env_profile, provider, load_category, reprocess_flag, batch_id, s3_input_files_prefix, step_function_info, step_function_execution_id, error_info, error_message, 'FATAL', sns_topic_arn, 'Glue-ETL-Pre-Auth Letters Storage Load')
        # raise Exception(error_message)
    else:
        status="COMPLETED"

        preauth_lib.preauth_step_logging(secrets, batch_id, step_id, None, None, status, 'Glue-ETL-Pre-Auth Letters Storage Load')
        logger.info(f"[SUCCESS] Successfully updated step_id {step_id} to status {status}.")
    logger.info(f"Processing COMPLETED for Batch ID {batch_id}!!!")
    if reprocess_flag.strip().upper()=='Y' and int(failed_batch_id) > 0 and status.upper().strip()=='COMPLETED':
        update_repro_stat=f""" UPDATE txm_preauth_logs.batch_preauth_master SET status='COMPLETED_IN_REPROCESS' WHERE batch_id='{failed_batch_id}' """
        preauth_lib.run_sql_query(update_repro_stat, secrets)
    elif reprocess_flag.strip().upper()=='Y' and int(failed_batch_id) ==-1  and status.upper().strip()=='COMPLETED':
        v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        logger.info(f"v_load_timestamp => {v_load_timestamp}")
        update_repro_stat=f""" UPDATE txm_preauth_logs.batch_preauth_master SET status='COMPLETED_IN_REPROCESS',end_datetime='{v_load_timestamp}',reprocess_flag='Y',file_type='NOTES' WHERE batch_id='{batch_id}'"""
        preauth_lib.run_sql_query(update_repro_stat, secrets)

except Exception as e:
    status="FAILED"

    logger.error(f"Error in PreAuth Letters Storage Load: {e}")
    error_info = "Failed in PreAuth Letters Storage Load"
    error_message = str(e)
    
    preauth_lib.preauth_error_logging(secrets, batch_id, step_id, None, status, 'PREAUTH LETTERS STORAGE GLUE FAILED', error_info, error_message, 'Glue-ETL-Pre-Auth Letters Storage Load', True)
    logger.info(f"[FAILURE] Logged failure for batch_id {batch_id} and step_id {step_id}")

    response = preauth_lib.log_and_email(env_profile, provider, load_category, reprocess_flag, batch_id, s3_input_files_prefix, step_function_info, step_function_execution_id, error_info, error_message, "FATAL", sns_topic_arn, "Glue-ETL-Pre-Auth Letters Storage Load")
    logger.info(f"Email sent successfully!message_id:{response['MessageId']}")

    raise 
finally:
    try:
        # Commit the job
        job.commit()
        logger.info("Job committed successfully")
        if reprocess_flag.strip().upper()=='Y' and int(failed_batch_id) > 0 and (status.upper().strip()=='COMPLETED' or status.upper().strip()=='FAILED_NO_DATA')  :
            update_repro_stat=f""" UPDATE txm_preauth_logs.batch_preauth_master SET status='COMPLETED_IN_REPROCESS' WHERE batch_id='{failed_batch_id}' """
            preauth_lib.run_sql_query(update_repro_stat, secrets)
        elif reprocess_flag.strip().upper()=='Y' and int(failed_batch_id) ==-1 and (status.upper().strip()=='COMPLETED' or status.upper().strip()=='FAILED_NO_DATA') :
            v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            logger.info(f"v_load_timestamp => {v_load_timestamp}")
            update_repro_stat=f""" UPDATE txm_preauth_logs.batch_preauth_master SET status='COMPLETED_IN_REPROCESS',end_datetime='{v_load_timestamp}',reprocess_flag='Y',file_type='LETTERS' WHERE batch_id='{batch_id}' """
            preauth_lib.run_sql_query(update_repro_stat, secrets)
    except Exception as e:
        logger.error(f"Error committing the job:{e}")
        raise