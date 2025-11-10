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
from pyspark.sql.functions import lit,count,udf,col,trim,coalesce,substring,split,substring_index,monotonically_increasing_id,concat_ws
from preauth_glue_commons import preauth_glue_common as preauth_lib
from pyspark.sql.types import StructType, StringType

#Configure logging.
logging.basicConfig(level=logging.INFO)
#set the logging level to INFO
logger = logging.getLogger("claimant-billingestion-pre-auth-notes-enhanced-load-job")

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, [ 'JOB_NAME',
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
    'retries',
    'retry_delay',
    'retry_multiplier',
    's3_bucket',
    's3_input_files_prefix',
    'input_file_name',
    'lambda_function_composite_api',
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


datacatalog_schema_name=args['datacatalog_schema_name'].strip()
datacatalog_config_table_name=args['datacatalog_config_table_name'].strip()
db_secret_key=args['db_secret_key'].strip()
batch_id=args['batch_id'].strip()
env_profile=args['env_profile'].strip()
sns_topic_arn=args['sns_topic_arn'].strip()
provider = args['provider'].strip()
load_category = args['load_category'].strip()
s3_error_attachment_prefix=args['s3_error_attachment_prefix'].strip()
error_alerts_to_email_ids=args['error_alerts_to_email_ids'].strip()
lambda_function_nodemailer=args['lambda_function_nodemailer'].strip()
lambda_function_claim_match=args['lambda_function_claim_match'].strip()
lambda_function_composite_api=args['lambda_function_composite_api'].strip()
retries=int(args['retries'])
retry_delay=int(args['retry_delay'])
retry_multiplier=int(args['retry_multiplier'])
bucket_name=args['s3_bucket'].strip()
s3_input_files_prefix=args['s3_input_files_prefix'].strip()
input_file_name=args['input_file_name'].strip()
reprocess_flag = args['reprocess_flag'].strip()
step_function_info = args.get('step_function_info')
step_function_execution_id = args.get('step_function_execution_id')
failed_batch_id = args.get('failed_batch_id')
updt_batch_id=batch_id
failed_step_id=None
mode = "append"
status="IN PROGRESS"
notes_file_header_id=0
reprocess_valid_list=['completed','completed_in_reprocess','skip_execution']
logging_table="txm_preauth_logs.batch_preauth_step_details"
error_logging_table="txm_preauth_logs.batch_preauth_error_details"
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
source = None
file_name = s3_input_files_prefix.strip('/').split('/')[-1]
logger.info(f"file name {file_name}...")
job_name = args['JOB_NAME']
is_execution=True
is_reprocess_exec=False  


def note_reprocess_template(claim_match_failure_df,process,env_profile, batch_id, source, provider, bucket_name, source_prefix, target_prefix, file_name):
    logger.info("inside note_reprocess_template")
    try:
        current_date = datetime.now().strftime('%Y-%m-%d')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        cst_time = datetime.now(pytz.timezone('America/Chicago')).strftime('%Y/%m/%d %H:%M:%SCST')
        source_prefix=source_prefix.replace('/error_logs','')
        target_prefix=target_prefix.replace('/error_logs','')
        claim_match_failed_source_prefix=f"{source_prefix}/claim_resubmit_template"
        claim_source_prefix_with_date = f"{claim_match_failed_source_prefix}/{current_date}/"
        claim_match_failed_target_prefix=f"{target_prefix}/claim_resubmit_template"
        claim_target_prefix_with_date = f"{claim_match_failed_target_prefix}/{current_date}/"
        logger.info(f'claim_match_failed_source_prefix : {claim_match_failed_source_prefix}')
        logger.info(f'claim_target_prefix_with_date : {claim_target_prefix_with_date}')
        claim_match_failure_df.show(10, truncate=False)
        FailedrecordCount = claim_match_failure_df.count()
        logger.info(f"Failed Claim Match Record Count => {FailedrecordCount}")
        claim_match_failure_df.coalesce(1).write.format('text').option('header','false').option("lineSep", "\r\n").mode('append').save(f"s3://{bucket_name}/{claim_source_prefix_with_date}")
        target_file_name_claim=f'TMI_{timestamp}_P_NN.txt'
        logger.info(f'TARGET FILE NAME Claim Match Failed: {target_file_name_claim}')
        s3_client = boto3.client('s3','us-east-1')
        response_claim_match = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=claim_source_prefix_with_date)
        if 'Contents' in response_claim_match and response_claim_match['Contents']:
            files = response_claim_match['Contents']
            files_sorted = sorted(files, key=lambda x: x['LastModified'], reverse=True)
            source_file = files_sorted[0]["Key"]
            logger.info(f'SOURCE_FILE : {source_file}')
            if source_file:
                target_file = f'{claim_target_prefix_with_date}{target_file_name_claim}'
                copy_source = {'Bucket': bucket_name, 'Key': source_file}
                s3_client.copy_object(CopySource=copy_source, Bucket=bucket_name, Key=target_file)
                s3_client.delete_object(Bucket=bucket_name, Key=source_file)
                logger.info(f"Renamed File {source_file} to {target_file}")
                # Invoke the Lambda function to send an email with the attachment
                target_file_name_claim=target_file
                logger.info(f"target_file_name_claim=>{target_file_name_claim}")
        else:
            logger.info("No objects found in the specified S3 path.")

    except Exception as e:
        logger.error(f"Error: {e}")
        raise
    return target_file_name_claim    

def log_and_email_attachment(failed_bills_log_df,claim_match_failure_df,process,env_profile, batch_id, source, provider, bucket_name, source_prefix, target_prefix, file_name):
    logger.info("inside log email attachment method")
    try:
        s3Keys=[]
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
        html=''
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
                target_file_email=target_file
                s3Keys.append(target_file_email)
                target_file_name_claim=None
                if claim_match_failure_df is not None :
                    target_file_name_claim=note_reprocess_template(claim_match_failure_df,process,env_profile, batch_id, source, provider, bucket_name, source_prefix, target_prefix, file_name)
                if  target_file_name_claim is not None :
                    s3Keys.append(target_file_name_claim)
                    html=f"""Attached Text file for the Invalid Claim Number(s) to be reprocessed."""
                logger.info(f"s3Keys=>{s3Keys}")    
                client = boto3.client('lambda',region_name='us-east-1')
                payload = {
                            's3Bucket': bucket_name,
                            's3Keys':s3Keys,
                            'to': error_alerts_to_email_ids,
                            'subject': f'{env_profile} - {source_tp_str} - {process}',
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
                preauth_lib.preauth_error_logging(secrets, batch_id, step_id, claim_number, status, 'CLAIM_MATCH_LAMBDA_INVOKE_FAILED', error_info, error_message, 'Glue - PreAuth Notes Enhanced Load', False)
            except Exception as log_error:
                logger.error(f"Failed to log error details: {log_error}")
            
            print(f"Retry delayed {delay} seconds for Attempt {attempt + 1}...")
            time.sleep(delay)
            delay *= retry_multiplier

    return claim_match_response_status


try:
    logger.info(f"db_secret_key => {db_secret_key}")
    secrets = preauth_lib.get_secret(db_secret_key, 'us-east-1')
    step_id = preauth_lib.preauth_step_logging(secrets, batch_id, None, job_name, job_id, status, 'Glue-ETL-Pre-Auth Notes Enhanced Load')
    logger.info(f"Step record inserted with step_id => {step_id}")
    
    if reprocess_flag.upper().strip()=='N' and batch_id:
       dup_chk=f""" SELECT count(*) as cnt FROM txm_preauth.preauth_notes_data WHERE batch_id={batch_id} """
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
            if load_category.lower() == 'notes':
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
                        del_failed_batch_data=f"""DELETE FROM txm_preauth.preauth_notes_data  where batch_id={failed_batch_id}"""
                        preauth_lib.run_sql_query(del_failed_batch_data, secrets)
                        is_execution=True
                        is_reprocess_exec=True
                        batch_id=failed_batch_id
    if reprocess_flag.strip().upper()=='Y' and int(failed_batch_id) > 0 :
        batch_id=failed_batch_id
    
    if is_execution:                   
        logger.info(f"Processing STARTED for Pre Auth Notes Enhanced Load...")
        logger.info(f"Reading preauth Notes configuration from Data Catalog table => {datacatalog_schema_name}.{datacatalog_config_table_name}...")
        AWSGlueDataCatalog_EnhancedConfig = glueContext.create_dynamic_frame.from_catalog(database=datacatalog_schema_name, table_name=datacatalog_config_table_name, transformation_ctx="AWSGlueDataCatalog_EnhancedConfig")
        #AWSGlueDataCatalog_EnhancedConfig.printSchema()
        config_df = AWSGlueDataCatalog_EnhancedConfig.toDF()
        config_df=config_df.filter(config_df.load_category=='notes')
        config_rec = config_df.first()
        notes_file_header_id_df = preauth_lib.get_df_db_data_with_query(spark, config_rec["db_read_sql_select"].replace('<INPUT_BATCH_ID>', batch_id), secrets)
        notes_file_header_id_rec=notes_file_header_id_df.first()
        notes_file_header_id=notes_file_header_id_rec['notes_file_header_id']
        logger.info(f"Processing Enhance load for notes_file_header_id=>{notes_file_header_id}")
        # Check if data available for Input File Header ID
        logger.info(f'Reading Transformation configuration from Data Catalog table => {config_rec["datacatalog_schema_name"]}.{config_rec["datacatalog_transformation_table_name"]}...')
        AWSGlueDataCatalog_EnhancedTPConfig = glueContext.create_dynamic_frame.from_catalog(database=config_rec["datacatalog_schema_name"], table_name=config_rec["datacatalog_transformation_table_name"], transformation_ctx="AWSGlueDataCatalog_EnhancedTPConfig")
        #AWSGlueDataCatalog_EnhancedTPConfig.printSchema()
        tp_transformation_config_df = AWSGlueDataCatalog_EnhancedTPConfig.toDF()
        tp_transformation_config_df=tp_transformation_config_df.filter(tp_transformation_config_df.table_type=='NOTES_DATA')
        tp_transformation_config_df.show(1, truncate=False)
        recordCount = tp_transformation_config_df.count()
        logger.info(f"Trading Partner config Record Count => {recordCount}")
        tp_transformation_config_rec = tp_transformation_config_df.first()
        notes_enhance_df = preauth_lib.get_df_db_data_with_query(spark, tp_transformation_config_rec["transform_sql_select"].replace('<notes_file_header_id>', str(notes_file_header_id)), secrets)
        
        #Validations of Notes starts 
        trailer_sql=f"""SELECT COUNT(claim_number) AS claim_count,ifnull(nfth.record_count,0) AS trailer_count FROM txm_preauth_staging.preauth_notes_file_header nfh
                        LEFT OUTER JOIN txm_preauth_staging.preauth_notes_header nh ON nh.notes_file_header_id=nfh.notes_file_header_id
                        LEFT OUTER JOIN txm_preauth_staging.preauth_notes_file_trailer nfth ON nfth.notes_file_header_id=nfh.notes_file_header_id
                        WHERE batch_id='<INPUT_BATCH_ID>'"""
        
        pre_trailer_validations_notes_df = preauth_lib.get_df_db_data_with_query(spark, trailer_sql.replace('<INPUT_BATCH_ID>', str(batch_id)), secrets)
        pre_trailer_validations_notes_df.printSchema()
        pre_trailer_validations_notes_df.show(1,truncate=False)
        pre_trailer_validations_notes_rec=pre_trailer_validations_notes_df.first()
        trailer_count=pre_trailer_validations_notes_rec["trailer_count"]
        claim_count=pre_trailer_validations_notes_rec["claim_count"]
        logger.info(f"trailer count=>{trailer_count} claim_count=> {claim_count}")
        if(trailer_count !=0 and trailer_count==claim_count):
            pre_validations_notes_df = preauth_lib.get_df_db_data_with_query(spark, tp_transformation_config_rec["db_read_sql_select"].replace('<INPUT_BATCH_ID>', str(batch_id)), secrets)
            notes_enhance_df.show(5,truncate=False)
            claimMatchUDF = udf(invoke_claim_match,StringType())
            logger.info('Invoking Claim Match Lambda for Validations...')
            pre_validations_notes_df = pre_validations_notes_df.withColumn("validation_result",claimMatchUDF(col("claim_number"),col("date_of_injury"),lit(batch_id),lit(step_id),lit(retries),lit(retry_delay),lit(retry_multiplier))) 
            pre_validations_notes_df = pre_validations_notes_df.cache()
            pre_validations_notes_df_claim_count = pre_validations_notes_df.count()
            logger.info(f"Claim Match API Response Record Count => {pre_validations_notes_df_claim_count}")
            v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            pre_validations_notes_pass_df=pre_validations_notes_df.filter(pre_validations_notes_df.validation_result=='PASS')
            logger.info(f"Count of Claim Match Pass Count=>{pre_validations_notes_pass_df.count()}")
            pre_validations_notes_df=pre_validations_notes_df.filter(pre_validations_notes_df.validation_result=='FAIL')
            logger.info(f"Count of Claim Match Fail Count=>{pre_validations_notes_df.count()}")
            pre_validations_notes_pass_df.show(5,truncate=False)
            claim_validation_df =pre_validations_notes_df.withColumn("error_description", lit("Invalid Claim Number")) \
                                    .withColumn("field_name_1", lit("preauth_notes_data.claim_number")) \
                                    .withColumn("field_value_1",col("claim_number")) \
                                    .withColumn("field_name_2", lit('preauth_notes_data.source_type')) \
                                    .withColumn("field_value_2", col('activity_name')) \
                                    .withColumn("update_timestamp", lit(v_load_timestamp)) \
                                    .withColumn("datetime_received", lit(v_load_timestamp))
            claim_validation_df=claim_validation_df.drop(claim_validation_df.date_of_injury).drop(claim_validation_df.notes_file_header_id).drop(claim_validation_df.validation_result)
            claim_validation_df.printSchema()
            claim_validation_df.show(5,truncate=False)
            process='Preauth Notes Validations'
            failed_record_cnt= claim_validation_df.count()
            logger.info(f"Records with failed!=>{pre_validations_notes_df.count()}")
            archive_path=s3_input_files_prefix.replace('preauth_code_files','archive')
            s3_path=f"s3://{bucket_name}/{archive_path}"
            logger.info(f"s3_path => {s3_path}")
            init_df=spark.read.text(s3_path).toDF("pre_auth_data_str").withColumn("unique_id", monotonically_increasing_id())
            header_df=init_df.filter(trim(substring(init_df.pre_auth_data_str, 1, 1))=='H').withColumn('order',lit(1))
            trailer_df=init_df.filter(trim(substring(init_df.pre_auth_data_str, 1, 1))=='T').withColumn('order',lit(3))
            init_df_i=init_df.filter(trim(substring(init_df.pre_auth_data_str, 1, 1))=='I').withColumn('claim_number',trim(substring(init_df.pre_auth_data_str, 2, 20))).withColumn("source_type", trim(substring(init_df.pre_auth_data_str, 130, 50))).select(col('claim_number'),col('pre_auth_data_str'),col('unique_id'),col('source_type'))
            init_df_i.printSchema()
            init_df_c=init_df.filter(trim(substring(init_df.pre_auth_data_str, 1, 1))=='C')
            logger.info(f"Claim=>")
            init_df_c.show(5,truncate=False)
            init_df_c=init_df_c.withColumn("claim_number", trim(substring(split(col("pre_auth_data_str"), "\\|")[0],2,20))).withColumn("source_type",split(col("pre_auth_data_str"), "\\|")[2]).withColumn("pre_auth_data_str",split(col("pre_auth_data_str"), "\\|")[3])
            init_df_c.printSchema()
            init_df_c=init_df_c.select(col('claim_number'),col('pre_auth_data_str'),col('unique_id'),col('source_type'))
            init_df=init_df_i.union(init_df_c)
            init_df=init_df.withColumn('poisitionchar',trim(substring(init_df.pre_auth_data_str, 1, 1)))
            init_df.printSchema()
            init_df.show(truncate=False)
            logger.info(f"Records before join")
            pre_validations_notes_df.show(10,truncate=False)
            init_df.select(col('claim_number'),col('source_type')).show(10,truncate=False)
            reject_df=init_df.join(pre_validations_notes_df,(pre_validations_notes_df.claim_number==init_df.claim_number) & (trim(pre_validations_notes_df.activity_name)==trim(init_df.source_type)),'inner').select(trim(col('pre_auth_data_str')),col('poisitionchar'),init_df.claim_number,col('unique_id')).withColumn('order',lit(2))
            reject_df=reject_df.distinct()
            reject_df=reject_df.orderBy(["claim_number","poisitionchar",'unique_id'], ascending=[True,False,True])
            pre_trailer_cnt_df=reject_df.filter(trim(substring(init_df.pre_auth_data_str, 1, 1))=='I')
            trailer_record_cnt=str(pre_trailer_cnt_df.count()).zfill(6)
            trailer_df=trailer_df.withColumn('pre_auth_data_str',concat_ws('', lit('T'),lit(trailer_record_cnt)))
            trailer_df.show(1,truncate=False)
            trailer_record_cnt=pre_trailer_cnt_df.count()
            logger.info(f"reject df-->{reject_df.count()}")
            reject_df.show(5,truncate=False)
            reject_df=reject_df.drop(col('claim_number')).drop(col('poisitionchar'))
            reject_df=header_df.union(reject_df).union(trailer_df)
            reject_df=reject_df.orderBy(["order",'unique_id'], ascending=[True,True]).drop(col('order')).drop(col('unique_id'))
            #reject_df.show(truncate=False)
            if int(failed_record_cnt) > 0:
                response = log_and_email_attachment(claim_validation_df,reject_df,process, env_profile, batch_id, source, provider, bucket_name, s3_error_attachment_prefix, s3_error_attachment_prefix, file_name)
            logger.info(f"response for attachment!=>{response}")
            AWSGlueDataCatalog_Notes_source_type_Config = glueContext.create_dynamic_frame.from_catalog(database='preauth_static_config', table_name='notes_source_type', transformation_ctx="AWSGlueDataCatalog_Notes_source_type_Config")
            notes_source_type_df = AWSGlueDataCatalog_Notes_source_type_Config.toDF()
            notes_source_type_df=notes_source_type_df.withColumnRenamed('activityrelatedto','activity_related_to')\
                                                     .withColumnRenamed('noterelatedto','note_related_to')\
                                                     .withColumnRenamed('notesubject','note_subject')\
                                                     .withColumnRenamed('notetopic','note_topic')\
                                                     .withColumnRenamed('sourcetype','source_type')
            notes_source_type_df.persist()
            notes_enhance_null_df=notes_enhance_df.join(notes_source_type_df,notes_source_type_df.source_type==notes_enhance_df.source_type,'left')\
                                             .join(pre_validations_notes_pass_df,pre_validations_notes_pass_df.claim_number==notes_enhance_df.claim_number,'inner')\
                                             .select(notes_enhance_df.claim_number,notes_source_type_df.source_type,col('note_timestamp'),col('note_body'),col('note_activity_source'),col('process_by_cc_status'),col('map_type'),col('activity_pattern_code'),col('activity_related_to'),col('note_subject'),col('note_related_to'),col('note_topic'),col('claim_id'),col('exposure_public_id'))
            notes_enhance_null_df=notes_enhance_null_df.fillna('DEFAULT',subset=["source_type","map_type","activity_pattern_code"])  
            logger.info(f"setting Null values with DEFAULT!")
            notes_enhance_not_default_df=notes_enhance_null_df.filter(notes_enhance_null_df.source_type!='DEFAULT')
            notes_source_type_default_df=notes_source_type_df.filter(notes_source_type_df.source_type =='DEFAULT')
            notes_enhance_default_df=notes_enhance_null_df.filter(notes_enhance_null_df.source_type =='DEFAULT')
            notes_enhance_default_df.show(5,truncate=False)
            notes_enhance_default_df=notes_enhance_default_df.drop(col('activity_pattern_code'))\
                                                             .drop(col('map_type'))\
                                                             .drop(col('activity_related_to'))\
                                                             .drop(col('note_subject'))\
                                                             .drop(col('note_related_to'))\
                                                             .drop(col('note_topic'))
            notes_enhance_default_df = notes_enhance_default_df.withColumnRenamed("source_type", "source_types")
            notes_enhance_df_final=notes_enhance_default_df.join(notes_source_type_default_df,notes_source_type_default_df.source_type==notes_enhance_default_df.source_types,'inner')
            notes_enhance_df_final=notes_enhance_df_final.select(notes_enhance_df_final.claim_number,notes_enhance_df_final.source_type,col('note_timestamp'),col('note_body'),col('note_activity_source'),trim(col('process_by_cc_status')).alias("process_by_cc_status"),trim(col('map_type')).alias("map_type"),trim(col('activity_pattern_code')).alias("activity_pattern_code"),trim(col('activity_related_to')).alias("activity_related_to"),trim(col('note_subject')).alias("note_subject"),trim(col('note_related_to')).alias("note_related_to"),trim(col('note_topic')).alias("note_topic"),col('claim_id'),col('exposure_public_id'))
    
            notes_enhance_df_final.show(5,truncate=False)
            notes_enhance_df_final=notes_enhance_df_final.union(notes_enhance_not_default_df)
            logger.info(f"Record Count to process to Enhance load=>{notes_enhance_df_final.count()}")
            notes_enhance_df=notes_enhance_df_final\
            .withColumn("batch_id", lit(batch_id)) \
            .withColumn("create_user", lit("GlueETL-PreAuth-Notes-Enhance-load")) \
            .withColumn("update_user", lit("GlueETL-PreAuth-Notes-Enhance-load")) \
            .withColumn("create_timestamp", lit(v_load_timestamp)) \
            .withColumn("update_timestamp", lit(v_load_timestamp))
            #notes_enhance_df.show(5,truncate=False)
            #notes_enhance_df.printSchema()
            notes_enhance_df=notes_enhance_df.distinct()
            preauth_lib.load_data_to_rds(notes_enhance_df, tp_transformation_config_rec["db_table_name"], secrets, mode)
            batch_id=updt_batch_id
        else:
            logger.info(f"trailer count and claim count mismatch rejecting batch load!")
            v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            trailer_rec_missed_df =pre_trailer_validations_notes_df.select(pre_trailer_validations_notes_df.trailer_count)\
                                    .withColumn("Claim Number", lit('')) \
                                    .withColumn("error_description", lit("Trailer Record Missing")) \
                                    .withColumn("field_name_1", lit("preauth_notes_file_trailer.record_count")) \
                                    .withColumn("field_value_1",  lit('0')) \
                                    .withColumn("field_name_2", lit('')) \
                                    .withColumn("field_value_2", lit('')) \
                                    .withColumn("update_timestamp", lit(v_load_timestamp)) 
            batch_out_of_balance_df =pre_trailer_validations_notes_df.select(pre_trailer_validations_notes_df.trailer_count)\
                                    .withColumn("Claim Number", lit('')) \
                                    .withColumn("error_description", lit("Batch Out of Balance")) \
                                    .withColumn("field_name_1", lit("preauth_notes_file_trailer.record_count")) \
                                    .withColumn("field_value_1", col("trailer_count")) \
                                    .withColumn("field_name_2", lit('')) \
                                    .withColumn("field_value_2", lit('')) \
                                    .withColumn("update_timestamp", lit(v_load_timestamp))                         
            process='Preauth Notes Validations'
            if trailer_count==0:
                error_df=trailer_rec_missed_df.distinct()
                response = log_and_email_attachment(error_df,None, process, env_profile, batch_id, source, provider, bucket_name, s3_error_attachment_prefix, s3_error_attachment_prefix, file_name)
                raise Exception ('Trailer Record Count is Zero Aborting Flow!')
            else:
                error_df=batch_out_of_balance_df.distinct()
                response = log_and_email_attachment(error_df,None, process, env_profile, batch_id, source, provider, bucket_name, s3_error_attachment_prefix, s3_error_attachment_prefix, file_name)
                raise Exception ('Trailer Record Missed/Mismatch in claim/Trailer Count Aborting Flow!')
        status='COMPLETED'
        format_sql=f""" UPDATE txm_preauth.preauth_notes_data SET note_body = REPLACE(note_body, '----', '\n') WHERE batch_id={batch_id} """
        preauth_lib.run_sql_query(format_sql, secrets)
        if is_reprocess_exec:
            reprocess_status='COMPLETED_IN_REPROCESS'
            logger.info('Updating through inbuilt method!')
            preauth_lib.preauth_step_logging(secrets, failed_batch_id, failed_step_id, None, None, reprocess_status, 'Glue-ETL-Pre-Auth Notes Enhanced Reprocess Load')
        batch_id=updt_batch_id    
        logger.info(f"batch_id=>{batch_id}")
        preauth_lib.preauth_step_logging(secrets, batch_id, step_id, None, None, status, 'Glue-ETL-Pre-Auth Notes Enhanced Load')
        logger.info(f"[SUCCESS] Successfully updated step_id {step_id} to status {status}.")

except Exception as e:
    status="FAILED"
    batch_id=updt_batch_id
    logger.info(f" Except block-batch_id=>{batch_id}")
    logger.error(f"Error in PreAuth Enhanced Load: {e}")
    error_info = "Failed in PreAuth Enhanced Load"
    error_message = str(e)
    
    preauth_lib.preauth_error_logging(secrets, batch_id, step_id, None, status, 'PREAUTH NOTES ENHANCED GLUE FAILED', error_info, error_message, 'Glue-ETL-Pre-Auth Notes Enhanced Load', True)
    logger.info(f"[FAILURE] Logged failure for batch_id {batch_id} and step_id {step_id}")

    response = preauth_lib.log_and_email(env_profile, provider, load_category, reprocess_flag, batch_id, s3_input_files_prefix, step_function_info, step_function_execution_id, error_info, error_message, "FATAL", sns_topic_arn, "Glue-ETL-Pre-Auth Notes Enhanced Load")
    logger.info(f"Email sent successfully!message_id:{response['MessageId']}")

    raise 

try:
    # Commit the job
    job.commit()
    logger.info("Job committed successfully")
except Exception as e:
    logger.error(f"Error committing the job:{e}")
    raise