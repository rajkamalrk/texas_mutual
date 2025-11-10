import sys
import logging
import json
import os
import io
import zipfile
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
from pyspark.sql.functions import lit, col, lower, udf, when, concat, trim, substring, monotonically_increasing_id, substring_index
from preauth_glue_commons import preauth_glue_common as preauth_lib
from pyspark.sql.types import StructType, StringType

#Configure logging.
logging.basicConfig(level=logging.INFO)
#set the logging level to INFO
logger = logging.getLogger("claimant-billingestion-preauth-letters-enhanced-load-job")

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, [ 'JOB_NAME',
    's3_bucket',
    's3_input_files_prefix',
    'datacatalog_schema_name',
    'datacatalog_config_table_name',
    'db_secret_key',
    'batch_id',
    'env_profile',
    'sns_topic_arn',
    'provider',
    'load_category',
    's3_error_attachment_prefix',
    'error_alerts_to_email_ids',
    'lambda_function_nodemailer',
    'lambda_function_claim_match',
    'reprocess_flag',
    'step_function_info',
    'step_function_execution_id',
    'retries',
    'retry_delay',
    'retry_multiplier',
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
datacatalog_schema_name=args['datacatalog_schema_name'].strip()
datacatalog_config_table_name=args['datacatalog_config_table_name'].strip()
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
lambda_function_claim_match=args['lambda_function_claim_match'].strip()
reprocess_flag = args['reprocess_flag'].strip()
failed_batch_id = args['failed_batch_id'].strip()
step_function_info = args.get('step_function_info')
step_function_execution_id = args.get('step_function_execution_id')
retries=int(args['retries'])
retry_delay=int(args['retry_delay'])
retry_multiplier=int(args['retry_multiplier'])
failed_step_id=None
updt_batch_id=batch_id
mode = "append"
status="IN PROGRESS"
logging_table="txm_preauth_logs.batch_preauth_step_details"
error_logging_table="txm_preauth_logs.batch_preauth_error_details"
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
file_name = s3_input_files_prefix.strip('/').split('/')[-1]
logger.info(f"file name {file_name}...")
source = None
final_zip_path="logs/letters_enhanced_load_glue/claim_resubmit_template/"
part_file_key = None
target_zip_file = None
final_zip_key = None

def log_and_email_attachment(failed_bills_log_df,process,env_profile, batch_id, source, provider, bucket_name, source_prefix, target_prefix, target_zip_file, file_name):
    logger.info("inside log email attachment method")
    try:
        current_date = datetime.now().strftime('%Y-%m-%d')
        timestamp = datetime.now().strftime('%m%d%Y')
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
        html=''

        if 'Contents' in response and response['Contents']:
            files = response['Contents']
            files_sorted = sorted(files, key=lambda x: x['LastModified'], reverse=True)
            source_file = files_sorted[0]["Key"]
            logger.info(f'SOURCE_FILE : {source_file}')

            if source_file:
                target_error_file = f'{target_prefix_with_date}{target_file_name}'
                copy_source = {'Bucket': bucket_name, 'Key': source_file}
                s3_client.copy_object(CopySource=copy_source, Bucket=bucket_name, Key=target_error_file)
                s3_client.delete_object(Bucket=bucket_name, Key=source_file)
                logger.info(f"Renamed File {source_file} to {target_error_file}")

                s3_keys_list = [target_error_file]
                if target_zip_file:
                    s3_keys_list.append(target_zip_file)
                    html=f"""Attached Zipped file for the Invalid Claim Number(s) to be reprocessed."""

                # Invoke the Lambda function to send an email with the attachment
                client = boto3.client('lambda',region_name='us-east-1')
                file_name = file_name + '.zip'
                payload = {
                    's3Bucket': bucket_name,
                    's3Keys': s3_keys_list,
                    'to': error_alerts_to_email_ids,
                    'subject': f'{env_profile.upper()} - {source_tp_str} - {process}',
                    'html': f"""<p>Please find the attached error log for {source_tp_str} with batch ID {batch_id}.</p>
                                <p>{html}</p>
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


def invoke_claim_match(claim_number, date_of_injury, batch_id, step_id, retries, retry_delay, retry_multiplier):
    print('Invoke Claim Match lambda function')
    
    payload = {
        "claimNumber": claim_number,
        "dateOfInjury": str(date_of_injury),
        "batchId": batch_id,
        "stepId": step_id
    }

    print(f"Payload to Lambda: {json.dumps(payload)}")
    delay = retry_delay
    claim_match_response_status=None

    boto_config = Config(connect_timeout=120, read_timeout=300)
    lambda_client = boto3.client('lambda', region_name='us-east-1', config=boto_config)
    for attempt in range(0,retries + 1):
        print(f"Attempt: {attempt}")
        try:
            response = lambda_client.invoke(
                FunctionName=lambda_function_claim_match,
                InvocationType='RequestResponse',
                Payload=json.dumps(payload)
            )
            response_payload = response['Payload'].read().decode('utf-8')
            print(f"Lambda response payload: {response_payload}")

            # Parse response
            response_json = json.loads(response_payload)
            status_code = response_json.get('statusCode', '')
            body_json = response_json.get('body', '')
            if isinstance(body_json, str):
                body_json = json.loads(body_json)

            message = body_json.get('message', '')
            response_claim = body_json.get('claimNumber', '')

            if status_code == 200 and response_claim == claim_number:
                claim_match_response_status = "PASS"
                break

            else:
                claim_match_response_status = "FAIL"
                print(f"Unexpected response. StatusCode: {status_code}, Message: {message}, Claim: {response_claim}")
                print(f"Retry delayed {delay} seconds for Attempt {attempt + 1}...")
                time.sleep(delay)
                delay *= retry_multiplier

        except Exception as e:
            print(f"Error invoking Lambda function: {str(e)}")
            claim_match_response_status = 'FAIL'
            error_info = f"Error occurred while invoking Claim Match lambda function (Attempt - {attempt})"
            error_message = str(e).replace("'", "''").replace("\\", "\\\\")
            print(f"error_info => {error_info}")  
            print(f"Error: {error_message}")
            
            # Log error details using the preauth error logging format
            try:
                preauth_lib.preauth_error_logging(secrets, batch_id, step_id, claim_number, status, 'CLAIM_MATCH_LAMBDA_INVOKE_FAILED', error_info, error_message, 'Glue - PreAuth Letters Enhanced Load', False)
            except Exception as log_error:
                logger.error(f"Failed to log error details: {log_error}")
            
            print(f"Retry delayed {delay} seconds for Attempt {attempt + 1}...")
            time.sleep(delay)
            delay *= retry_multiplier

    return claim_match_response_status

def invalid_claim_zip(claim_df,image_df,s3_bucket,file_name,s3_input_files_prefix,final_zip_path):
    logger.info(f"Zip invalid claim number...")
    s3 = boto3.client('s3')
    try:
        txt_file_list = preauth_lib.get_s3_objects(s3_bucket, s3_input_files_prefix, '.txt')
        if len(txt_file_list) > 1:
            logger.info("containg more than 1 .txt files")
            raise
        if not txt_file_list:
            logger.error("No .txt file found in the input S3 path.")
            raise
        txt_file_path = txt_file_list[0]
        txt_file_name = txt_file_path.split('/')[-1]
        logger.info(f"txt_file_name => {txt_file_name}")
        zip_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        new_zip_file_name = f"TMI_IDX_{zip_timestamp}"
        logger.info(f"new zip file name => {new_zip_file_name}")
        s3_path=f"s3://{s3_bucket}/{s3_input_files_prefix}{txt_file_name}"
        template_folder = f"{final_zip_path}{new_zip_file_name}/"
        template_s3_path = f"s3://{s3_bucket}/{template_folder}"
        logger.info(f"Original file path: {s3_path}")
        logger.info(f"Template write folder: {template_s3_path}")
        init_df=spark.read.text(s3_path).toDF("pre_auth_data_str")
        init_df.printSchema()
        init_df.show(5,truncate=False)

        image_df = image_df.select(col('image_name'))
        image_name_iterator = image_df.rdd.toLocalIterator()
    
        # Move attachments from the source folder to the template folder
        for row in image_name_iterator:
            image_name = row['image_name']
            source_key = s3_input_files_prefix + image_name
            destination_key = template_folder + image_name
            
            s3.copy_object(CopySource={'Bucket': s3_bucket, 'Key': source_key}, Bucket=s3_bucket, Key=destination_key)
            logger.info(f"Moved attachment {image_name} to template folder.")

        init_df_indexed = init_df.withColumn("target_substring",
            when(substring(col("pre_auth_data_str"), 1, 1) == "C",trim(substring(col("pre_auth_data_str"), 2, 29)))
            .when(substring(col("pre_auth_data_str"), 1, 1) == "I",substring_index(trim(substring(col("pre_auth_data_str"), 88, 70)), '_', 1))
            .otherwise(lit(None))).withColumn( "row_id", monotonically_increasing_id())

        claim_df = claim_df.select(col("claim_number")).distinct()
        join_condition = [init_df_indexed["target_substring"]== claim_df["claim_number"]]
        joined_df = init_df_indexed.join(claim_df,join_condition,"inner")

        filtered_df = joined_df.select(col("pre_auth_data_str"), col("row_id")).distinct().sort(col("row_id"))
        logger.info(f"Filtered lines count: {filtered_df.count()}")
        filtered_df = filtered_df.select(col("pre_auth_data_str"))
        filtered_df.show(5, truncate=False)

        filtered_df.coalesce(1).write.mode("append").option("compression", "none").text(template_s3_path)
        logger.info(f"Filtered content written to template location: {template_s3_path}")

        # List objects in the target prefix (folder)
        template_objects = preauth_lib.get_s3_objects(s3_bucket, template_folder, None)
        if not template_objects:
            logger.error("template folder is empty after writing and moving attachments. Aborting cleanup.")
            return None
    
        for key in template_objects:
            file_name_only = os.path.basename(key)
            if file_name_only.startswith('part-') and file_name_only.endswith('.txt'):
                part_file_key = key
                break

        if part_file_key: # Rename the created part file
            txt_timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            new_txt_file = f"TMI_IDX_{txt_timestamp}" + ".txt"
            destination_prefix = template_folder + new_txt_file
            preauth_lib.move_s3_file(destination_prefix, s3_bucket, part_file_key)

        # Zip and upload the file in template folder
        keys_to_zip = []
        zip_buffer = io.BytesIO()
        zip_list_objects = preauth_lib.get_s3_objects(s3_bucket, template_folder, None)
        
        if zip_list_objects:
            keys_to_zip = [key for key in zip_list_objects if not key.endswith('/')]
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
                for key in keys_to_zip:
                    response = s3.get_object(Bucket=s3_bucket, Key=key)
                    file_content = response['Body'].read()
                    zf.writestr(os.path.basename(key), file_content)
            zip_buffer.seek(0)
            
            # Upload the final zip to the error template folder
            final_zip_key = final_zip_path + new_zip_file_name + ".zip"
            s3.upload_fileobj(zip_buffer, s3_bucket, final_zip_key)
            logger.info(f"Successfully uploaded final ZIP to s3://{s3_bucket}/{final_zip_key}")

            # Delete the entire template folder
            keys_to_delete = [{'Key': k} for k in keys_to_zip]
            s3.delete_objects(Bucket=s3_bucket, Delete={'Objects': keys_to_delete})
            logger.info(f"Cleaned up {len(keys_to_delete)} template files from {template_folder}")
        else:
            logger.error("Could not find the final renamed text file to zip.")
    except Exception as e:
        logger.error(f"FATAL ERROR during invalid_claim_zip processing: {e}", exc_info=True)
        return None
    
    return final_zip_key

is_validation = False
is_transformation = False
is_validation_errors=False
is_transform_errors=False
is_skip_execution = False
is_no_data_execution = False
is_no_document_execution = False
datetime_received = None
new_zip_file_name = None
log_status = ""
mode = "append"
is_execution=True
is_reprocess_exec=False  

try:
    logger.info(f"Processing STARTED for Pre Auth Letters Enhanced Load...")
    logger.info(f"db_secret_key => {db_secret_key}")
    secrets = preauth_lib.get_secret(db_secret_key, 'us-east-1')
    step_id = preauth_lib.preauth_step_logging(secrets, batch_id, None, job_name, job_id, status, 'Glue-ETL-Pre-Auth Letters Enhanced Load')
    logger.info(f"Step record inserted with step_id => {step_id}")
    reprocess_valid_list=['completed','completed_in_reprocess','skip_execution']
    if reprocess_flag.upper().strip()=='N' and batch_id:
       dup_chk=f""" SELECT count(*) as cnt FROM txm_preauth.preauth_letters_data WHERE batch_id={batch_id} """
       dup_chkstatus=preauth_lib.run_sql_query(dup_chk, secrets)
       dup_chk_cnt= int(dup_chkstatus[0]['cnt'])
       if(dup_chk_cnt) > 0:
           is_execution=False
           raise Exception("Duplicate Batch!.Batch is already Processed.")
    
    if reprocess_flag.upper().strip()=='Y':
        validate_sql=f"""SELECT  m.batch_id,st.status,st.step_id,m.file_name,file_type FROM txm_preauth_logs.batch_preauth_step_details st
                     INNER JOIN   txm_preauth_logs.batch_preauth_master m ON m.batch_id=st.batch_id
                     WHERE m.batch_id={failed_batch_id}  AND job_name='{job_name}' """
        validation_enhance_status=preauth_lib.run_sql_query(validate_sql, secrets)
        print(f"validation_enhance_status => {validation_enhance_status}")
        
        if validation_enhance_status:
            load_category=validation_enhance_status[0]['file_type'].lower()
            failed_step_id=validation_enhance_status[0]['step_id']
            if load_category.lower() == 'letters':
                if isinstance(validation_enhance_status, list):
                    if str(validation_enhance_status[0]['status']).lower() in reprocess_valid_list:
                        print(f"Batch=> {failed_batch_id} with file name =>{validation_enhance_status[0]['file_name']} is already processed succesfully in the last run.Terminating Reprocess!")
                        is_execution=False
                        v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                        update_current_batch_id_stat=f"""UPDATE txm_preauth_logs.batch_preauth_step_details  st
                                            INNER JOIN   txm_preauth_logs.batch_preauth_master m ON m.batch_id=st.batch_id
                                            SET st.status='SKIP_EXECUTION',st.end_datetime='{v_load_timestamp}'
                                            WHERE m.batch_id={batch_id}  AND job_name='{job_name}'"""
                        preauth_lib.run_sql_query(update_current_batch_id_stat, secrets)
                    else:
                        del_failed_batch_data=f"""DELETE FROM txm_preauth.preauth_letters_data  where batch_id={failed_batch_id}"""
                        preauth_lib.run_sql_query(del_failed_batch_data, secrets)
                        is_execution=True
                        is_reprocess_exec=True
                        batch_id=failed_batch_id
                        
    if reprocess_flag.strip().upper()=='Y' and int(failed_batch_id) > 0 :
        batch_id=failed_batch_id
        
    if is_execution:                
        logger.info(f"Reading preauth configuration from Data Catalog table => {datacatalog_schema_name}.{datacatalog_config_table_name}...")
        AWSGlueDataCatalog_EnhancedConfig = glueContext.create_dynamic_frame.from_catalog(database=datacatalog_schema_name, table_name=datacatalog_config_table_name, transformation_ctx="AWSGlueDataCatalog_EnhancedConfig")
        AWSGlueDataCatalog_EnhancedConfig.printSchema()
        AWSGlueDataCatalog_EnhancedConfig.show()
        config_df = AWSGlueDataCatalog_EnhancedConfig.toDF()
        config_rec = config_df.filter(lower(col("load_category")) == load_category.lower()).first()
        logger.info(f"config_rec => {config_rec}")
    
        #Validating Enhanced table data and loaded into a dataframe
        record_list_df = preauth_lib.get_df_db_data_with_query(spark, config_rec["db_read_sql_select"].replace('<INPUT_BATCH_ID>', batch_id), secrets)
        record_list_df = record_list_df.cache()
        record_list_df.createOrReplaceTempView(config_rec["df_table_name"])
        record_list_df.printSchema()
        record_list_df.show(5, truncate=False)
        record_list_count = record_list_df.count()
        logger.info(f"Record Count => {record_list_count}")

        sql_query = f"""
            SELECT t1.batch_id, t1.header_id, t2.detail_id, t2.image_name, t2.datetime_received, t1.claim_number, t3.document_serial_number 
            FROM txm_preauth_staging.preauth_letters_header t1
            JOIN txm_preauth_staging.preauth_letters_detail t2 ON t1.header_id = t2.header_id
            LEFT JOIN txm_preauth.preauth_letters_data t3 ON t2.image_name = t3.document_name AND t3.document_serial_number IS NOT NULL
            WHERE t3.document_name IS NOT NULL AND t2.image_name IS NOT NULL AND t1.batch_id = {batch_id}"""

        processed_document_df = preauth_lib.get_df_db_data_with_query(spark, sql_query, secrets)
        processed_document_df.printSchema()
        processed_document_df.show(5, truncate=False)
        processed_document_count = processed_document_df.count()
        logger.info(f"Document already send to nuxeo => {processed_document_count}")

        join_condition = [record_list_df['image_name'] == processed_document_df['image_name']]
        final_processed_df = record_list_df.join(processed_document_df, join_condition, 'left_anti')
        final_processed_df.printSchema()
        final_processed_df.show(5, truncate=False)
        final_processed_count = final_processed_df.count()
        logger.info(f"New Document count => {final_processed_count}")
    
        # Check if data available for Batch ID
        if final_processed_count > 0:
            logger.info(f"Data Available for Batch ID {batch_id}...")
            logger.info(f"Pre Auth validations starts...")
    
            is_validation = True
            v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            logger.info(f"v_load_timestamp => {v_load_timestamp}")

            # Filtering images already send to nuxeo.
            nuxeo_processed_df = processed_document_df.withColumn('validation_result', lit('FAIL')) \
                                .withColumn("error_description", lit("Doc. Processed Prior")) \
                                .withColumn("field_name_1", lit("preauth_letters_header.claim_number")) \
                                .withColumn("field_value_1", col("claim_number")) \
                                .withColumn("field_name_2", lit("preauth_letters_header.image_name")) \
                                .withColumn("field_value_2", col("image_name")) \
                                .withColumn("update_timestamp", lit(v_load_timestamp)) \
                                .select(col("batch_id"),col("header_id"),col("detail_id"),col("image_name"),col("datetime_received"),col("claim_number"),col("error_description"),col("validation_result"),col("field_name_1"),col("field_value_1"),col("field_name_2"), col("field_value_2"), col("update_timestamp"))
            nuxeo_processed_df.printSchema()
            nuxeo_processed_df.show(5, truncate=False)
            nuxeo_processed_count = nuxeo_processed_df.count()
            logger.info(f"Document already send to nuxeo count => {nuxeo_processed_count}")
    
            claim_validation_df_1=final_processed_df.select("claim_number", "date_of_injury").distinct()
            # Performance fix for claim validation UDF call, Previous line reducing the number partitions in DF            
            logger.info(f"Setting claim_validation_df_1 partition count as => 100")
            claim_validation_df_1 = claim_validation_df_1.repartition(100)
            logger.info(f"claim_validation_df_1 NEW partition count => {claim_validation_df_1.rdd.getNumPartitions()}")
            claim_validation_df_1=claim_validation_df_1.cache()
            claim_validation_df_1.printSchema()
            claim_validation_df_1.show(5, truncate=False)
            claim_validation_count = claim_validation_df_1.count()
            logger.info(f"Claim matches Record Count => {claim_validation_count}")
    
            claimMatchUDF = udf(invoke_claim_match,StringType())
    
            logger.info('Invoking Claim Match Lambda for Validations...')
            claim_match_response_df = claim_validation_df_1.withColumn("validation_result",claimMatchUDF(col("claim_number"),col("date_of_injury"),lit(batch_id),lit(step_id),lit(retries),lit(retry_delay),lit(retry_multiplier))) 
            claim_match_response_df = claim_match_response_df.cache()
            claim_match_response_df.printSchema()
            claim_match_response_df.show(5, truncate=False)
            claim_match_response_count = claim_match_response_df.count()
            logger.info(f"Claim Match API Response Record Count => {claim_match_response_count}")
    
            # claim number validations.
            claim_validation_df_2 = final_processed_df.withColumn("error_description", lit("Invalid Claim Number")) \
                                .withColumn("field_name_1", lit("preauth_letters_header.claim_number")) \
                                .withColumn("field_value_1", col("claim_number")) \
                                .withColumn("field_name_2", lit("preauth_letters_header.image_name")) \
                                .withColumn("field_value_2", col("image_name")) \
                                .withColumn("update_timestamp", lit(v_load_timestamp)) \
                                .select(col("batch_id"),col("header_id"),col("detail_id"),col("image_name"),col("datetime_received"),col("claim_number"),col("error_description"),col("field_name_1"),col("field_value_1"),col("field_name_2"), col("field_value_2"), col("update_timestamp"))
            claim_validation_df_2=claim_validation_df_2.cache()
            claim_validation_df_2.printSchema()
            claim_validation_df_2.show(5, truncate=False)
            df_meta = claim_validation_df_2.alias("meta")
            df_valid = claim_match_response_df.alias("valid")
    
            join_condition = [df_meta['claim_number'] == df_valid['claim_number']]
            final_claim_validation_df = df_meta.join(df_valid,join_condition,"inner")
            final_claim_validation_df = final_claim_validation_df.select(col("batch_id"),col("header_id"),col("detail_id"),col("image_name"),col("datetime_received"),col("meta.claim_number"),col("error_description"),col("validation_result"),col("field_name_1"),col("field_value_1"),col("field_name_2"), col("field_value_2"), col("update_timestamp"))
            final_claim_validation_df=final_claim_validation_df.cache()
            final_claim_validation_df.printSchema()
            final_claim_validation_df.show(5, truncate=False)
            claim_validation_count = final_claim_validation_df.count()
            logger.info(f"Claim matches for batch Record Count => {claim_validation_count}")
            claim_match_failure_df = final_claim_validation_df.filter(final_claim_validation_df["validation_result"] == "FAIL")
            claim_match_failure_df.printSchema()
            claim_match_failure_df.show(5, truncate=False)
            claim_match_failure_count = claim_match_failure_df.count()
            logger.info(f"Claim Match Failure Record Count => {claim_match_failure_count}")
    
            # validation : Doc File not found in s3
            #s3 document validations starts
            letters_documents_df = final_processed_df.withColumn("error_description", lit("Doc File not found")) \
                                            .withColumn("field_name_1", lit("preauth_letters_detail.image_name")) \
                                            .withColumn("field_value_1", col("image_name")) \
                                            .withColumn("field_name_2", lit('')) \
                                            .withColumn("field_value_2", lit('')) \
                                            .withColumn("update_timestamp", lit(v_load_timestamp))
            letters_documents_df=letters_documents_df.cache()
            letters_documents_df.printSchema()
            letters_documents_df.show(5, truncate=False)
            letters_documents_count = letters_documents_df.count()
            logger.info(f"Billing Documents Record Count => {letters_documents_count}")
            datetime_received = letters_documents_df.first()["datetime_received"]
            logger.info(f"datetime_received => {datetime_received}")
            
            s3_doc_list = preauth_lib.get_s3_objects(bucket = s3_bucket, prefix = s3_input_files_prefix, suffix = None)
            logger.info(f"s3_bill_doc_list => {s3_doc_list}")
            if s3_doc_list and letters_documents_count > 0:
                s3_docs_df=spark.createDataFrame(s3_doc_list, "string").toDF("s3_doc_path")
                s3_docs_df=s3_docs_df.withColumn('s3_doc_path',concat(lit(f"s3://{s3_bucket}/"),col("s3_doc_path")))
                s3_docs_df = s3_docs_df.filter(~col("s3_doc_path").rlike("\.txt$"))
                s3_docs_df.printSchema()
                s3_docs_df.show(5, truncate=False)
                recordCount = s3_docs_df.count()
                logger.info(f"S3 Docs Record Count => {recordCount}")
    
                doc_file_validation_df=letters_documents_df.join(s3_docs_df, col('s3_doc_path').contains(col('image_name')), 'inner')
                doc_file_pass_validation_df = doc_file_validation_df.withColumn('validation_result', lit('PASS')) \
                    .select(col("batch_id"),col("header_id"),col("detail_id"),col("datetime_received"),col("claim_number"),col("error_description"),col("validation_result"),col("field_name_1"),col("field_value_1"),col("field_name_2"), col("field_value_2"), col("update_timestamp"))
                doc_file_pass_validation_df.printSchema()
                doc_file_pass_validation_df.show(5, truncate=False)
                recordCount = doc_file_pass_validation_df.count()
                logger.info(f"validation letters Documents Record Count => {recordCount}")
                
                join_condition = [letters_documents_df['image_name'] == doc_file_validation_df['image_name']]
                doc_file_fail_validation_df = letters_documents_df.join(doc_file_validation_df, join_condition, 'left_anti')
                doc_file_fail_validation_df = doc_file_fail_validation_df.withColumn('validation_result', lit('FAIL')) \
                            .select(col("batch_id"),col("header_id"),col("detail_id"),col("image_name"),col("datetime_received"),col("claim_number"),col("error_description"),col("validation_result"),col("field_name_1"),col("field_value_1"),col("field_name_2"), col("field_value_2"), col("update_timestamp"))
                doc_file_fail_validation_df.printSchema()
                doc_file_fail_validation_df.show(5, truncate=False)
                validation_errors_count = doc_file_fail_validation_df.count()
                logger.info(f"Missing Records Count => {validation_errors_count}")
    
                #Invalid letters Type Validation
                valid_types = ['PDF', 'HTML', 'TIFF', 'DOC', 'DOCX', 'DOCM', 'PPT', 'PPTX', 'XLS', 'XLSX', 'XLSM', 'RTF', 'TXT']
                letters_type_validation_df = final_processed_df \
                    .withColumn("error_description", lit("Invalid Letters Type")) \
                    .withColumn("validation_result", when(col("image_type").isin(valid_types), "PASS").otherwise("FAIL")) \
                    .withColumn("field_name_1", lit("image_type")) \
                    .withColumn("field_value_1", col("image_type")) \
                    .withColumn("field_name_2", lit("preauth_letters_detail.image_name")) \
                    .withColumn("field_value_2", col("image_name")) \
                    .withColumn("update_timestamp", lit(v_load_timestamp)) \
                    .withColumn("datetime_received", col("datetime_received")) \
                    .select(col("batch_id"),col("header_id"),col("detail_id"),col("image_name"),col("datetime_received"),col("claim_number"),col("error_description"),col("validation_result"),col("field_name_1"),col("field_value_1"),col("field_name_2"), col("field_value_2"), col("update_timestamp"))
                    
                letters_type_validation_df=letters_type_validation_df.cache()
                letters_type_validation_df.printSchema()
                letters_type_validation_df.show(5, truncate=False)
                letters_type_validation_count = letters_type_validation_df.count()
                logger.info(f"Letters Type Count => {letters_type_validation_count}")
    
                #Orphan Attachments Validation
                orphan_attachments_df = s3_docs_df.join(letters_documents_df,col('s3_doc_path').contains(col('image_name')),'left_anti')
                orphan_attachments_df = orphan_attachments_df.join(nuxeo_processed_df,col('s3_doc_path').contains(col('image_name')),'left_anti')
                orphan_attachments_df = orphan_attachments_df.withColumn('validation_result', lit('FAIL')) \
                            .withColumn('batch_id',lit(batch_id)) \
                            .withColumn('header_id',lit(None)) \
                            .withColumn('detail_id', lit(None)) \
                            .withColumn('image_name', lit(None)) \
                            .withColumn('claim_number', lit('NA')) \
                            .withColumn('datetime_received', lit(datetime_received)) \
                            .withColumn('error_description', lit('Orphan Attachments')) \
                            .withColumn('validation_result', lit('FAIL')) \
                            .withColumn('field_name_1', lit('S-3 File path')) \
                            .withColumn('field_value_1', col('s3_doc_path')) \
                            .withColumn('field_name_2', lit('')) \
                            .withColumn('field_value_2', lit('')) \
                            .withColumn('update_timestamp',lit(v_load_timestamp)) \
                            .select(col("batch_id"),col("header_id"),col("detail_id"),col("image_name"),col("datetime_received"),col("claim_number"),col("error_description"),col("validation_result"),col("field_name_1"),col("field_value_1"),col("field_name_2"), col("field_value_2"), col("update_timestamp"))
                orphan_attachments_df.printSchema()
                orphan_attachments_df.show(5, truncate=False)
                orphan_attachments_count = orphan_attachments_df.count()
                logger.info(f"Orphan Attachments Count => {orphan_attachments_count}")
    
                final_validation_df = doc_file_fail_validation_df.unionByName(letters_type_validation_df, allowMissingColumns=False) \
                            .unionByName(orphan_attachments_df, allowMissingColumns=False) \
                            .unionByName(final_claim_validation_df, allowMissingColumns=False) \
                            .unionByName(nuxeo_processed_df, allowMissingColumns=False)
                final_validation_df.printSchema()
                final_validation_df.show(5, truncate=False)
                final_validation_count = final_validation_df.count()
                logger.info(f"Final validation Count => {final_validation_count}")

                claim_zip_validation_df = claim_match_failure_df.select(col("claim_number")).distinct()
                claim_zip_validation_df.printSchema()
                claim_zip_validation_df.show(10, truncate=False)
                claim_zip_validation_count = claim_zip_validation_df.count()
                logger.info(f"Final claim zip validation Count => {claim_zip_validation_count}")

                join_condition = [claim_match_failure_df['image_name'] == doc_file_fail_validation_df['image_name']]
                image_zip_validation_df = claim_match_failure_df.join(doc_file_fail_validation_df, join_condition, 'left_anti')
                image_zip_validation_df = image_zip_validation_df.select(col("image_name")).distinct()
                image_zip_validation_df.printSchema()
                image_zip_validation_df.show(10, truncate=False)
                image_zip_validation_count = image_zip_validation_df.count()
                logger.info(f"Final image zip validation Count => {image_zip_validation_count}")
                if claim_match_failure_count > 0:
                    target_zip_file=invalid_claim_zip(claim_zip_validation_df,image_zip_validation_df, s3_bucket,file_name,s3_input_files_prefix,final_zip_path)
                    logger.info(f"zip file path => {target_zip_file}")
    
                validation_result_df = final_validation_df.filter(final_validation_df["validation_result"] == "PASS")
                validation_result_df.printSchema()
                validation_result_df.show(5, truncate=False)
                validation_result_count = validation_result_df.count()
                logger.info(f"Final Pass Validation Count => {validation_result_count}")
    
                validation_errors_df = final_validation_df.filter(final_validation_df["validation_result"] == "FAIL")
                validation_errors_df.printSchema()
                validation_errors_df.show(5, truncate=False)
                validation_errors_count = validation_errors_df.count()
                logger.info(f"Final Fail Validation Count => {validation_errors_count}")
    
                if validation_errors_count > 0:
                    #email part
                    logger.info(f"Found detail_ids with Failed Validations...")
                    
                    failed_bills_log_df=validation_errors_df \
                        .select(col("datetime_received"),col("claim_number"),col("error_description"),col("field_name_1"),col("field_value_1"),col("field_name_2"), col("field_value_2"), col("update_timestamp"))
                    failed_bills_log_df.printSchema()
                    failed_bills_log_df.show(5, truncate=False)
                    recordCount = failed_bills_log_df.count()
                    logger.info(f"Failed Bills Logs DF result Record Count => {recordCount}")
    
                    logger.info(f"Found Letters Attachment Validation Errors. Sending Consolidated Error mails with error details with attach...")
                    process='Preauth Letters Validations'
                    response = log_and_email_attachment(failed_bills_log_df, process, env_profile, batch_id, source, provider, s3_bucket, s3_error_attachment_prefix, s3_error_attachment_prefix, target_zip_file, file_name)
                    logger.info(f"ErrorLog File Created successfully: {response}")
    
                logger.info(f'Reading Transformation configuration from Data Catalog table => {config_rec["datacatalog_schema_name"]}.{config_rec["datacatalog_transformation_table_name"]}...')
                AWSGlueDataCatalog_EnhancedTPConfig = glueContext.create_dynamic_frame.from_catalog(database=config_rec["datacatalog_schema_name"], table_name=config_rec["datacatalog_transformation_table_name"], transformation_ctx="AWSGlueDataCatalog_EnhancedTPConfig")
                AWSGlueDataCatalog_EnhancedTPConfig.printSchema()
                AWSGlueDataCatalog_EnhancedTPConfig.show()
                tp_transformation_config_df = AWSGlueDataCatalog_EnhancedTPConfig.toDF()
                tp_transformation_config_df.show(3, truncate=False)
                recordCount = tp_transformation_config_df.count()
                logger.info(f"Trading Partner config Record Count => {recordCount}")
                preauth_config_df=tp_transformation_config_df.filter(tp_transformation_config_df.table_type=='LETTERS_DATA')
                tp_transformation_config_itr = preauth_config_df.orderBy(['order_seq_no']).rdd.toLocalIterator()
                is_validation = False
                is_transformation = True
                
                #Processing Trading Partner Transformation configuration in order sequence
                for row in tp_transformation_config_itr:
                    logger.info(f"Processing started for row: {row}")
                    table_names = [name.strip() for name in row.load_config_table_list.split(',') if name.strip()]
                    for static_tblname in table_names:
                        logger.info(f"Processing table: {static_tblname}")
                        if '.' in static_tblname:
                            database, table = static_tblname.split(".")
                            logger.info(f"Database: {database}, Table: {table}")
                            AWSGlueDataCatalog_Enhanced_Static_Config = glueContext.create_dynamic_frame.from_catalog(
                            database=database,
                            table_name=table,
                            transformation_ctx=f"AWSGlueDataCatalog_Enhanced_Static_Config_{table}"
                            )
                            AWSGlueDataCatalog_Enhanced_Static_Config.printSchema()
                            AWSGlueDataCatalog_Enhanced_Static_Config.show()
                
                            static_table_df = AWSGlueDataCatalog_Enhanced_Static_Config.toDF().cache()
                
                        # Create a temporary view
                            view_name = f"{table}_vw"
                            static_table_df.createOrReplaceTempView(view_name)
                            logger.info(f"Created temporary view: {view_name}")
                            result = spark.sql(f"SELECT COUNT(*) as record_count FROM {view_name}")
                            logger.info(f"Record count for {static_tblname}: {result.first()['record_count']}")
                    
                        else:
                            logger.error(f"Invalid table name format: {static_tblname}")
                            raise Exception(f"Invalid table name format: {static_tblname}")
                    logger.info("Processing completed for all tables")
                    
                    #Reading each table data from Staging DB and loaded into a dataframe
                    enhanced_df = preauth_lib.get_df_db_data_with_query(spark, row["db_read_sql_select"].replace('<INPUT_BATCH_ID>', batch_id), secrets)
                    enhanced_df = enhanced_df.cache()
                    enhanced_df.printSchema()
                    enhanced_df.show(5, truncate=False)
                    enhancedRecordCount = enhanced_df.count()
                    logger.info(f"Enhanced load DB read result Record Count => {enhancedRecordCount}")
    
                    enhanced_df = enhanced_df.join(s3_docs_df, col('s3_doc_path').contains(col('image_name')), 'left')
                    enhanced_df = enhanced_df.cache()
    
                    if validation_errors_count > 0:
                        #Filter validation passed bill records alone (Use enhanced_df left anti join validation_error_df)
                        logger.info("Filtering Enhanced DF to skip Validation failed Bills...")
                        join_condition = [enhanced_df['detail_id'] == validation_errors_df['detail_id']]
                        enhanced_filtered_df = enhanced_df.join(validation_errors_df, join_condition, 'leftanti').cache()
                        enhanced_filtered_df.printSchema()
                        enhanced_filtered_df.show(5, truncate=False)
                        enhancedFilteredRecordCount = enhanced_filtered_df.count()
                        logger.info(f"Enhanced load DB read result (exclude validation errors) Record Count => {enhancedFilteredRecordCount}")
                    else:
                        logger.info("No Validation Errors found in the Batch.")
                        enhanced_filtered_df = enhanced_df
    
                    enhanced_filtered_df.createOrReplaceTempView(row["df_read_table_name"])
    
                    logger.info(f'Transform SQL...')
                    enhanced_transform_df = spark.sql(row["transform_sql_select"])
                    enhanced_transform_df.printSchema()
                    enhanced_transform_df.show(5, truncate=False)
                    recordCount = enhanced_transform_df.count()
                    logger.info(f"Enhanced Transform Record Count => {recordCount}")
                    
                    logger.info(f"Adding ETL processing columns...")
                    v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                    logger.info(f"v_load_timestamp => {v_load_timestamp}")
                    enhanced_transform_df = enhanced_transform_df \
                        .withColumn("create_user", lit("Glue ETL - Enhance loading")) \
                        .withColumn("update_user", lit("Glue ETL - Enhance loading")) \
                        .withColumn("create_timestamp", lit(v_load_timestamp)) \
                        .withColumn("update_timestamp", lit(v_load_timestamp))
                    enhanced_transform_df.printSchema()
                    enhanced_transform_df.show(1, truncate=False)
                    recordCount = enhanced_transform_df.count()
                    logger.info(f"Record Type Data Transform Record Count => {recordCount}")
    
                    logger.info(f'Load transformed dataframe to RDS table => {row["db_table_name"]}')
                    preauth_lib.load_data_to_rds(enhanced_transform_df, row["db_table_name"], secrets, mode)
    
                    #file name changes in s3
                    sql_query = f"""
                        SELECT distinct d.detail_id, e.document_name, d.image_name, h.batch_id 
                        from txm_preauth_staging.preauth_letters_header h 
                        join txm_preauth_staging.preauth_letters_detail d ON h.header_id = d.header_id 
                        join txm_preauth.preauth_letters_data e on d.detail_id = e.detail_id 
                        where e.document_name <> d.image_name and h.batch_id = {batch_id}"""
    
                    doc_name_change_df = preauth_lib.get_df_db_data_with_query(spark, sql_query, secrets)
                    doc_name_change_df.printSchema()
                    doc_name_change_df.show(5, truncate=False)
                    doc_name_change_count = doc_name_change_df.count()
                    logger.info(f"Document name changes in s3 Count => {doc_name_change_count}")
    
                    if doc_name_change_count > 0:
                        #Register udf
                        logger.info(f'document file name changes start...')
                        NameChangeUDF = udf(preauth_lib.rename_s3_object,StringType())
                        df_with_rename_results = doc_name_change_df.withColumn("response",NameChangeUDF(lit(s3_bucket),lit(s3_input_files_prefix),col("image_name"),col("document_name"))) 
                        df_with_rename_results = df_with_rename_results.cache()
                        df_with_rename_results.show(truncate=False)
                        total_renamed_attempts = df_with_rename_results.count()
    
                    else:
                        logger.info(f'No document file name changes...')
            else:
                is_no_document_execution = True
                logger.info(f"No Letters Avaialble For Batch ID {batch_id}. Skipping the rest of the flow!!!")
    
        else:
            is_no_data_execution = True
            logger.info(f"No Data Avaialble Batch ID {batch_id}. Skipping the rest of the flow!!!")
    
        #Data for log table
        if is_no_data_execution:
            status="FAILED NO DATA"
            #Error Flow
            error_message = f"CustomError: Data NOT Available in Staging for Batch ID {batch_id}."
            logger.error(f"error_message => {error_message}")
            preauth_lib.log_and_email(env_profile, provider, load_category, reprocess_flag, batch_id, s3_input_files_prefix, step_function_info, step_function_execution_id, 'Glue Failed', 'FAILED NO DATA', 'FATAL', sns_topic_arn, 'Glue-ETL-Pre-Auth Letters Enhanced Load')
            raise Exception(error_message)
        elif is_no_document_execution:
            status="FAILED NO LETTERS FOUND"
            #Error Flow
            error_message = f"CustomError: Letters NOT Available in Staging for Batch ID {batch_id}."
            logger.error(f"error_message => {error_message}")
            preauth_lib.log_and_email(env_profile, provider, load_category, reprocess_flag, batch_id, s3_input_files_prefix, step_function_info, step_function_execution_id, 'Glue Failed', 'FAILED NO LETTERS FOUND', 'FATAL', sns_topic_arn, 'Glue-ETL-Pre-Auth Letters Enhanced Load')
            raise Exception(error_message)
    
        else:
            status="COMPLETED"
        
        if is_reprocess_exec:
            reprocess_status='COMPLETED_IN_REPROCESS'
            logger.info('Updating through inbuilt method!')
            preauth_lib.preauth_step_logging(secrets, failed_batch_id, failed_step_id, None, None, reprocess_status, 'Glue-ETL-Pre-Auth Letters Enhanced Reprocess Load')
        batch_id=updt_batch_id    
        logger.info(f"batch_id=>{batch_id}")
        preauth_lib.preauth_step_logging(secrets, batch_id, step_id, None, None, status, 'Glue-ETL-Pre-Auth Letters Enhanced Load')
        logger.info(f"[SUCCESS] Successfully updated step_id {step_id} to status {status}.")
        logger.info(f"Processing COMPLETED for Batch ID {batch_id}!!!")

except Exception as e:
    status="FAILED"

    logger.error(f"Error in PreAuth Enhanced Load: {e}")
    error_info = "Failed in PreAuth Enhanced Load"
    error_message = str(e)
    batch_id=updt_batch_id
    preauth_lib.preauth_error_logging(secrets, batch_id, step_id, None, status, 'PREAUTH LETTERS ENHANCED GLUE FAILED', error_info, error_message, 'Glue-ETL-Pre-Auth Letters Enhanced Load', True)
    logger.info(f"[FAILURE] Logged failure for batch_id {batch_id} and step_id {step_id}")

    response = preauth_lib.log_and_email(env_profile, provider, load_category, reprocess_flag, batch_id, s3_input_files_prefix, step_function_info, step_function_execution_id, error_info, error_message, "FATAL", sns_topic_arn, "Glue-ETL-Pre-Auth Letters Enhanced Load")
    logger.info(f"Email sent successfully!message_id:{response['MessageId']}")

    raise 

try:
    # Commit the job
    job.commit()
    logger.info("Job committed successfully")
except Exception as e:
    logger.error(f"Error committing the job:{e}")
    raise