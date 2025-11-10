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
from pyspark.sql.functions import lit,count,udf,col,trim,coalesce,when,isnan,isnull
from preauth_glue_commons import preauth_glue_common as preauth_lib
from pyspark.sql.types import StructType, StringType

#Configure logging.
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("claimant-billingestion-pre-auth-notes-CC-load-job")
#set the logging level to INFO

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, [ 'JOB_NAME',
    'db_secret_key',
    'env_profile',
    'retries',
    'retry_delay',
    'retry_multiplier',
    'lambda_function_composite_api',
    'step_function_info',
    'step_function_execution_id',
    'batch_id',
    'provider',
    's3_error_attachment_prefix',
    'sns_topic_arn',
    's3_input_files_prefix',
    's3_bucket',
    'error_alerts_to_email_ids',
    'lambda_function_nodemailer',
    'reprocess_flag',
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
source=None
db_secret_key=args['db_secret_key'].strip()
env_profile=args['env_profile'].strip()
lambda_function_composite_api=args['lambda_function_composite_api'].strip()
retries=int(args['retries'])
retry_delay=int(args['retry_delay'])
retry_multiplier=int(args['retry_multiplier'])
step_function_info = args.get('step_function_info')
step_function_execution_id = args.get('step_function_execution_id')
provider = args.get('provider')
batch_id = args.get('batch_id')
s3_error_attachment_prefix = args.get('s3_error_attachment_prefix')
sns_topic_arn = args.get('sns_topic_arn')
s3_input_files_prefix = args.get('s3_input_files_prefix')
bucket_name=args.get('s3_bucket')
error_alerts_to_email_ids=args.get('error_alerts_to_email_ids')
reprocess_flag = args['reprocess_flag'].strip()
lambda_function_nodemailer=args.get('lambda_function_nodemailer')
failed_batch_id=args.get('failed_batch_id')
status="IN PROGRESS"
file_name = s3_input_files_prefix.strip('/').split('/')[-1]
logging_table="txm_preauth_logs.batch_preauth_step_details"
error_logging_table="txm_preauth_logs.batch_preauth_error_details"
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

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
            .withColumnRenamed('claim_number','Claim Number') \
            .withColumnRenamed('error_description','Error Description') \
            .withColumnRenamed('field_name_1','Field name 1') \
            .withColumnRenamed('field_value_1','Field Value 1') \
            .withColumnRenamed('field_name_2','Field Name 2') \
            .withColumnRenamed('field_value_2','Field Value 2') \
            .withColumnRenamed('update_timestamp','TimeStamp') \
            .select(col('Claim Number'),col('Error Description'),col('Field Name 1'),col('Field Value 1'),col('Field Name 2'),col('Field Value 2'),col('TimeStamp'))

        #failed_bills_error_log_df.printSchema()
        failed_bills_error_log_df.show(10, truncate=False)
        recordCount = failed_bills_error_log_df.count()
        logger.info(f"Consolidated Error Mail Record Count => {recordCount}")

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
def invoke_composite_api(claim_number,data_id,batch_id,step_id,retries, retry_delay, retry_multiplier):
    print('Invoke composite lambda function')
    
    payload = {
        "claimNumber": claim_number,
	    "batchId": batch_id,
	    "stepId": step_id,
	    "dataId": data_id	
    }

    print(f"Payload to Lambda: {json.dumps(payload)}")
    delay = retry_delay
    composite_api_response_status=None

    boto_config = Config(connect_timeout=120, read_timeout=300)
    lambda_client = boto3.client('lambda', region_name='us-east-1', config=boto_config)
    for attempt in range(0,retries + 1):
        print(f"Attempt: {attempt}")
        try:
            response = lambda_client.invoke(
                FunctionName=lambda_function_composite_api,
                InvocationType='RequestResponse',
                Payload=json.dumps(payload)
            )
            response_payload = response['Payload'].read().decode('utf-8')
            logger.info(f"Lambda response payload: {response_payload}")

            # Parse response
            response_json = json.loads(response_payload)
            status_code = response_json.get('statusCode', '')
            body_json = response_json.get('body', '')
            if isinstance(body_json, str):
                body_json = json.loads(body_json)
            logger.info(f"response for {claim_number}==>{body_json}")
            message = body_json.get('message', '')
            response_claim = body_json.get('claimNumber', '')
            
            if status_code == 200 and response_claim == claim_number:
                composite_api_response_status = "PASS"
                break

            else:
                composite_api_response_status = "FAIL"
                print(f"Unexpected response. StatusCode: {status_code}, Message: {message}, Claim: {response_claim}")
                print(f"Retry delayed {delay} seconds for Attempt {attempt + 1}...")
                time.sleep(delay)
                delay *= retry_multiplier

        except Exception as e:
            print(f"Error invoking Lambda function: {str(e)}")
            composite_api_response_status = 'FAIL'
            errorInfo=f"Error occured while invoking Composite API lambda function (Attempt - {attempt})"
            errorMessage=str(e).replace("'","''").replace("\\","\\\\")
            print(f"errorInfo => {errorInfo}")  
            print(f"Error: {errorMessage}")
            
            # Log error details using the preauth error logging format
            try:
                preauth_lib.preauth_error_logging(secrets, batch_id, step_id, claim_number, status, 'COMPOSITE_API_LAMBDA_INVOKE_FAILED', errorInfo, errorMessage, 'Glue - PreAuth Notes CC Load', False)
            except Exception as log_error:
                logger.error(f"Failed to log error details: {log_error}")
            
            print(f"Retry delayed {delay} seconds for Attempt {attempt + 1}...")
            time.sleep(delay)
            delay *= retry_multiplier

    return composite_api_response_status

try:
    logger.info(f"db_secret_key => {db_secret_key}")
    secrets = preauth_lib.get_secret(db_secret_key, 'us-east-1')
    step_id = preauth_lib.preauth_step_logging(secrets, batch_id, None, job_name, job_id, status, 'Glue-ETL-Pre-Auth Notes process cc Load')
    logger.info(f"Step record inserted with step_id => {step_id}")
    notes_sql=f"""SELECT preauth_notes_data_id AS data_id ,claim_number  FROM txm_preauth.preauth_notes_data WHERE process_by_cc_status !='Y'"""
    notes_enhance_composite_df = preauth_lib.get_df_db_data_with_query(spark, notes_sql, secrets)
    compositeapiUDF = udf(invoke_composite_api,StringType())
    logger.info('Invoking UDF composite api...')
    notes_enhance_composite_udf_df = notes_enhance_composite_df.withColumn("composite_api_response",compositeapiUDF(col("claim_number"),col("data_id"),lit(batch_id),lit(step_id),lit(retries),lit(retry_delay),lit(retry_multiplier)))
    logger.info(f"Below is reponse from composite")
    notes_enhance_composite_udf_df=notes_enhance_composite_udf_df.persist()
    notes_enhance_composite_udf_df.show(5,truncate=False)
    cc_failed_claim_df=notes_enhance_composite_udf_df.filter(col("composite_api_response")=='FAIL')
    cc_failed_claim_df.show(5,truncate=False)
    if cc_failed_claim_df.count() > 0:
        v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        # Fetch error details from error table for the failed composite API calls
        composite_error_details_sql = f"""
            SELECT 
                COALESCE(err.claim_number, '') as claim_number,
                err.error_code,
                err.error_info,
                CONCAT(err.error_code, ' - ', IFNULL(REPLACE(CONVERT(err.error_message USING utf8), '"', ''''''), '')) AS error_description
            FROM {error_logging_table} err
            WHERE err.step_id = {step_id}"""
        composite_error_details_df = preauth_lib.get_df_db_data_with_query(spark, composite_error_details_sql, secrets)
        composite_error_details_df = composite_error_details_df.cache()
        composite_error_details_df.printSchema()
        composite_error_details_df.show(5, truncate=False)
        composite_error_details_count = composite_error_details_df.count()
        logger.info(f"Retrieved {composite_error_details_count} error detail records from error table for composite API")
        # Join the failed composite claims with error_details_df to get proper error descriptions
        if composite_error_details_count > 0:
            # Create aliases for the DataFrames
            failed_composite_alias = cc_failed_claim_df.alias("failed_composite")
            composite_error_details_alias = composite_error_details_df.alias("composite_error_details")
            # Join on claim_number to get error descriptions from error table
            joined_composite_errors_df = failed_composite_alias.join(
                composite_error_details_alias, 
                failed_composite_alias["claim_number"] == composite_error_details_alias["claim_number"], 
                "left"
            )
            # Select columns and use error_description from error table, fallback to default message if not found
            cc_failed_claim_df = joined_composite_errors_df.select(
                col("failed_composite.claim_number"),
                col("failed_composite.data_id"),
                col("failed_composite.composite_api_response"),
                # Use error_description from error table if available, otherwise use default message
                when(col("composite_error_details.error_description").isNull(), lit("Failed While Processing data to Guidewire Claim Center"))
                .otherwise(col("composite_error_details.error_description")).alias("error_description")
            ).withColumn("field_name_1", lit("preauth_notes_data.claim_number")) \
             .withColumn("field_value_1", col("claim_number")) \
             .withColumn("field_name_2", lit('')) \
             .withColumn("field_value_2", lit('')) \
             .withColumn("update_timestamp", lit(v_load_timestamp)) \
             .withColumn("datetime_received", lit(v_load_timestamp))
        else:
            #Fallback to original logic if no error details found in error table
            logger.info("No error details found in error table for composite API, using default error message")
            cc_failed_claim_df = cc_failed_claim_df.withColumn("error_description", lit("Failed While Processing data to Guidewire Claim Center")) \
                                .withColumn("field_name_1", lit("preauth_notes_data.claim_number")) \
                                .withColumn("field_value_1",col("claim_number")) \
                                .withColumn("field_name_2", lit('')) \
                                .withColumn("field_value_2", lit('')) \
                                .withColumn("update_timestamp", lit(v_load_timestamp)) \
                                .withColumn("datetime_received", lit(v_load_timestamp))
        
        cc_failed_claim_df=cc_failed_claim_df.drop(cc_failed_claim_df.data_id).drop(cc_failed_claim_df.composite_api_response).distinct()
        process='Processing Dairy Notes to Guidewire Claim Center'
            
        # Check for any errors in the error table for this batch and step
        error_table_sql = f"""
            SELECT 
                COALESCE(err.claim_number, '') as claim_number,
                err.error_code,
                err.error_info,
                CONCAT(err.error_code, ' - ', IFNULL(REPLACE(CONVERT(err.error_message USING utf8), '"', ''''''), '')) AS error_message,
                err.create_timestamp
            FROM {error_logging_table} err
            WHERE err.batch_id = '{batch_id}' AND err.step_id = {step_id}
        """
        error_table_df = preauth_lib.get_df_db_data_with_query(spark, error_table_sql, secrets)
        error_count = error_table_df.count()
        logger.info(f"Retrieved {error_count} error records from error table for batch_id={batch_id}, step_id={step_id}")
        
        if error_count > 0:
            logger.info(f"Found errors in error table. Sending Consolidated Error Email!!!")
            
            # Prepare error data for consolidated email from error table
            error_df = error_table_df.filter(
                col("claim_number") != ""
            ).select(
                col("claim_number").alias("Claim Number"),
                lit("Error Table Error").alias("Error Description"),
                lit("Error Code").alias("Field name 1"),
                col("error_code").alias("Field Value 1"),
                col("error_info").alias("Field Name 2"),
                col("error_message").alias("Field Value 2"),
                col("create_timestamp").alias("TimeStamp")
            ).distinct()
            
            # Send consolidated error email
            process = 'Preauth Notes CC Load - Error Table Errors'
            if error_df.count() > 0 :
                response = log_and_email_attachment(error_df, process, env_profile, batch_id, source, provider, bucket_name, s3_error_attachment_prefix, s3_error_attachment_prefix, None)
                logger.info(f"Error Email sent successfully: {response}")
        else:
            logger.info(f"No errors found in error table for this batch and step!!!")
    status='COMPLETED'
    preauth_lib.preauth_step_logging(secrets, batch_id, step_id, None, None, status, 'Glue-ETL-Pre-Auth Notes CC Load')
    logger.info(f"[SUCCESS] Successfully updated step_id {step_id} to status {status}.")    
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
    logger.error(f"Error in PreAuth CC Load: {e}")
    error_info = "Failed in PreAuth CC Load"
    error_message = str(e)
    preauth_lib.preauth_error_logging(secrets, batch_id, step_id, None, status, 'PREAUTH NOTES ENHANCED GLUE FAILED', error_info, error_message, 'Glue-ETL-Pre-Auth Notes Enhanced Load', True)
    logger.info(f"[FAILURE] Logged failure for step_id {step_id}")
    response = preauth_lib.log_and_email(env_profile, provider, 'Notes', 'N', batch_id, 'step', step_function_info, step_function_execution_id, error_info, error_message, "FATAL", sns_topic_arn, "Glue-ETL-Pre-Auth Notes CC Load")
    logger.info(f"Email sent successfully!message_id:{response['MessageId']}")
    raise 
try:
    # Commit the job
    job.commit()
    logger.info("Job committed successfully")
except Exception as e:
    logger.error(f"Error committing the job:{e}")
    raise