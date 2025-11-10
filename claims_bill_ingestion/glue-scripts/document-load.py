import sys
import logging
import json
import base64
import boto3
import pytz
import pymysql
from datetime import datetime
from botocore.exceptions import ClientError
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit, col, when, expr, concat, count, split, regexp_extract
from pyspark.sql.types import DecimalType

#Configure logging.
logging.basicConfig(level=logging.INFO)
#set the logging level to INFO
logger = logging.getLogger("claimant-billingestion-document_load-job")

def get_secret(secret_name, region_name):
    """
    Retrives secrets from AWS secrets Manager,
    Raises exceptions for any errors encountered during the retrieval process.
    """
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    try:
        get_secret_value_response = client.get_secret_value(SecretId = secret_name)
    except ClientError as e:
        logger.error("Error retrieving secret '%s': %s", secret_name, str(e))
        raise e
    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response.get('SecretString', '')
    if not secret: 
        decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
        secret = decoded_binary_secret
    return json.loads(secret)

def read_s3_file(bucket_name, file_prefix):
    """Read an S3 file"""
    logger.info(f"file_prefix => {file_prefix}")
    s3_client = boto3.client('s3')
    file_from_s3 = s3_client.get_object(Bucket=bucket_name, Key=file_prefix)
    file_content = file_from_s3['Body'].read().decode('utf-8')
    logger.info(f"s3 object with prefix {file_prefix} successfully read")
    return file_content
    
def is_s3_file_exists(bucket_name, key):
    """
    Checks if the specified file exists in the S3 bucket.
    
    :param bucket_name: Name of the S3 bucket.
    :param key: Object key (path) of the file in the S3 bucket.
    :return: True if the file exists, False otherwise.
    """
    is_file_exists = False
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=key)
    if 'Contents' in response:
        is_file_exists = True
        logger.info(f"S3 response does have Key Contents. S3 file {key} EXISTS!!! \n response => {response}")
    else:
        is_file_exists = False
        logger.info(f"S3 response doesn't have Key Contents. S3 file {key} NOT EXISTS!!! \n response => {response}")
    return is_file_exists

def get_s3_folder(bucket, prefix, suffix):
    """
    Fetches S3 folder matching the given prefix and suffix.
    
    :param bucket: Name of the S3 bucket.
    :param prefix: Prefix to filter S3 folder.
    :param suffix: Suffix to filter S3 folder.
    :return: List of S3 folder that match the criteria.
    """
    s3_client = boto3.client('s3')
    s3_response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter='/')
    s3_folder = None

    if 'CommonPrefixes' in s3_response:
        for object_data in s3_response['CommonPrefixes']:
            folder_prefix = object_data['Prefix']
            print(folder_prefix)
            if not suffix or folder_prefix.endswith(suffix):
                s3_folder = folder_prefix
    else:
        logger.info(f"S3 response doesn't have Key CommonPrefixes. s3_response => {s3_response}")
    if s3_folder:
        logger.info(f"S3 files available for processing with prefix => {prefix} and suffix => {suffix}")
        logger.info(f"s3_folder => {s3_folder}")
    else:
        logger.info(f"No folder available for processing with prefix => {prefix} and suffix => {suffix}")
    return s3_folder

def get_s3_objects(bucket, prefix, suffix):
    """
    Fetches all S3 objects matching the given prefix and suffix.
    
    :param bucket: Name of the S3 bucket.
    :param prefix: Prefix to filter S3 objects.
    :param suffix: Suffix to filter S3 objects.
    :return: List of S3 object keys that match the criteria.
    """
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/')
    s3_files = []
    for response in response_iterator:
        if 'Contents' in response:
            for object_data in response['Contents']:
                key = object_data['Key']
                if not suffix or key.endswith(suffix):
                    s3_files.append(key)
        else:
            logger.info(f"S3 response doesn't have Key Contents. response => {response}")
    if s3_files:
        logger.info(f"List of files available for processing with prefix => {prefix} and suffix => {suffix}")
        logger.info(f"s3_files => {s3_files}")
    else:
        logger.info(f"No files available for processing with prefix => {prefix} and suffix => {suffix}")
    return s3_files

def load_data_to_rds(data_frame, dbTableName, secrets, mode):
    logger.info(f"dbTableName => {dbTableName}")
    url = f'jdbc:mysql://{secrets["host"]}:{secrets["port"]}/{secrets["dbClusterIdentifier"]}'
    logger.info(f"url => {url}")
    data_frame.write.format("jdbc") \
        .option("url", url) \
        .option("user", secrets["username"]) \
        .option("password", secrets["password"]) \
        .option("dbtable", dbTableName) \
        .option("truncate", "true") \
        .option("numPartitions", "5") \
        .mode(mode) \
        .save()
    logger.info(f"Successfully written to DB Table!!!")

def get_df_db_data_with_query(read_db_sql, secrets, df_num_partitions=100):
    try:
        jdbc_url = f'jdbc:mysql://{secrets["host"]}:{secrets["port"]}/{secrets["dbClusterIdentifier"]}'
        read_db_sql = f"""({read_db_sql}) AS db_vw"""
        logger.info(f"read_db_sql => {read_db_sql}")
        df_db_res = spark.read.format("jdbc") \
                        .option("url", jdbc_url) \
                        .option("dbtable", read_db_sql) \
                        .option("user", secrets["username"]) \
                        .option("password", secrets["password"]) \
                        .option("numPartitions", "5") \
                        .option("fetchsize", "1000") \
                        .load()
        if df_db_res.count() == 0:
            logger.info(f"No matching records found for input read db sql!!!")
        else:
            logger.info(f"df_db_res partition count => {df_db_res.rdd.getNumPartitions()}")
            logger.info(f"Setting df_db_res partition count as => {df_num_partitions}")
            df_db_res = df_db_res.repartition(df_num_partitions)
            logger.info(f"df_db_res NEW partition count => {df_db_res.rdd.getNumPartitions()}")
        return df_db_res
    except Exception as e:
        logger.error("Error executing DB reading: %s", str(e))
        raise e
    
def update_db_records(updated_df, secrets, main_table_name, stage_table_name, update_db_query):
    """
    Update records in the main table based on data from a stage table using provided query.
    """
    # Extracting values from secrets
    host = secrets["host"]
    database = 'txm_bitx'  
    user = secrets["username"]
    password = secrets["password"]
    port = secrets["port"]
    logger.info(f"Database: {database}")
    logger.info(f"Main Table Name: {main_table_name}")
    logger.info(f"Stage Table Name: {stage_table_name}")

    try:
        # Write updated_df to the stage table in the database
        updated_df.write.format("jdbc") \
            .option("url", f"jdbc:mysql://{host}:{port}/{database}") \
            .option("dbtable", stage_table_name) \
            .option("user", user) \
            .option("password", password) \
            .mode("overwrite") \
            .save()
            
        logger.info(f"Successfully wrote updated_df data to stage table: {stage_table_name}")
        logger.info(f"update_db_query => {update_db_query}")

        # Establish MySQL connection
        connection = pymysql.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        )
        with connection.cursor() as cursor:
            # Execute the provided query
            cursor.execute(update_db_query)
            connection.commit()
        logger.info("Query executed successfully.")

        # Drop the stage table (if it exists)
        with connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {stage_table_name}")
            connection.commit()
        logger.info(f"Dropped stage table: {stage_table_name}")
    except Exception as e:
        logger.error("Error executing DB Update Records Flow: %s", str(e))
        raise e
    finally:
        # Close MySQL connection
        if connection:
            connection.close()
        logger.info("Closed MySQL connection.")

def update_invoice_status(input_df, secrets, status_type, status):
    logger.info(f"Invoice Status table update STARTED for status_type {status_type} and status {status}")
    v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

    update_main_table_name = f"txm_bitx.invoice_status"
    logger.info(f"update_main_table_name => {update_main_table_name}")

    update_stage_table_name = f"{update_main_table_name}_{trading_partner}_{file_header_id}_stage"
    logger.info(f"update_stage_table_name => {update_stage_table_name}")

    update_query = f"""
        UPDATE {update_main_table_name} main
        JOIN {update_stage_table_name} stage ON main.txm_invoice_number = stage.txm_invoice_number
        SET main.status_type = '{status_type}'
        ,main.status = '{status}'
        ,main.update_user = 'Glue - ETL'
        ,main.update_timestamp = '{v_load_timestamp}'
    """

    logger.info(f"Calling update_db_records function to update {update_main_table_name} in database...")
    update_db_records(input_df, secrets, update_main_table_name, update_stage_table_name, update_query)
    logger.info(f"Invoice Status table update COMPLETED for status_type {status_type} and status {status}")
    
    logger.info(f"Invoice Step Status table load STARTED for status_type {status_type} and status {status}")
    update_main_table_name = f"txm_bitx.invoice_step_status"
    logger.info(f"update_main_table_name => {update_main_table_name}")

    update_stage_table_name = f"{update_main_table_name}_{trading_partner}_{file_header_id}_stage"
    logger.info(f"update_stage_table_name => {update_stage_table_name}")

    insert_query = f"""
        INSERT INTO {update_main_table_name} (status_id, status_type, step_info, step_status, create_user, update_user, create_timestamp, update_timestamp)
        SELECT s.status_id, '{status_type}', '{status}', '{status}', 'Glue - ETL', 'Glue - ETL', '{v_load_timestamp}', '{v_load_timestamp}'
        FROM txm_bitx.invoice_status s
        JOIN {update_stage_table_name} stage ON s.txm_invoice_number = stage.txm_invoice_number
    """
    logger.info(f"Calling update_db_records function to insert {update_main_table_name} in database...")
    update_db_records(input_df, secrets, update_main_table_name, update_stage_table_name, insert_query)
    logger.info(f"Invoice Step Status table load COMPLETED for status_type {status_type} and status {status}")

def call_db_stored_procedure(secrets, db_stored_procedure, db_sp_params, return_out_param_position):
    """
    Executes MySQL Stored Procedure using the provided secret key.
    """
    conn = pymysql.connect( 
        host = secrets["host"],
        database = secrets["dbClusterIdentifier"],
        port = secrets["port"],
        user = secrets["username"],
        password = secrets["password"],
        connect_timeout = 300
    )
    try:
        cursor = conn.cursor()
        out_param_value = None
        
        logger.info(f"Calling DB Stored Procedure {db_stored_procedure}...")
        logger.info(f"db_sp_params => {db_sp_params}")
        #cursor.execute('SET SESSION max_execution_time = 3600')
        cursor.execute('SET LOCAL net_read_timeout = 300')
        #cursor.execute('SET LOCAL net_read_timeout = 31536000')
        #cursor.execute('SET LOCAL wait_timeout = 28800')
        #cursor.execute('SET LOCAL interactive_timeout = 28800')
        cur_res = cursor.callproc(db_stored_procedure, db_sp_params)
        logger.info(f"cur_res => {cur_res}")
        logger.info(f"Stored Procedure Executed!!!")
        logger.info(f"Stored Procedure execution output:")
        logger.info(cursor.fetchall()) ###First resultset
        while cursor.nextset(): ###Remaining resultset
            for res in cursor.fetchall():
                logger.info(res)
                if any(str.startswith('ProcedureExecutionError:') for str in res):
                    raise Exception(f"Error while executing Stored Procedure {db_stored_procedure}!!!")
        if return_out_param_position:
            out_param_query = f'SELECT @_{db_stored_procedure}_{return_out_param_position}'
            logger.info(f"out_param_query => {out_param_query}")
            cur_res = cursor.execute(out_param_query)
            logger.info(f"cur_res => {cur_res}")
            out_res = cursor.fetchone() ###First resultset
            logger.info(f"out_res => {out_res}")
            if out_res:
                out_param_value = out_res[0]
        logger.info(f"out_param_value => {out_param_value}")
        conn.commit()
    except Exception as e:
        logger.error("Error executing DB Stored Procedure: %s", str(e))
        raise e
    finally:
        conn.close()
    return out_param_value 

def log_and_email(env_profile,errorInfo,file_header_id,bill_id,txm_invoice_number,source,trading_partner,errormessage,severity,sns_topic_arn):
     """logging error and sending error email"""
     logger.info("inside log email method")
     sns_client=boto3.client('sns',region_name='us-east-1')
     cst=pytz.timezone('America/Chicago')
     timestamp=datetime.now(cst).strftime('%Y/%m/%d %H:%M:%S %Z%z')
     if trading_partner and trading_partner.strip() and trading_partner.lower() != "null":
        source_tp_str = trading_partner
     else:
       source_tp_str = source
     source_tp_str = source_tp_str.upper()
     #create log message
     process=f"Process : {source_tp_str} - SOLUTIONS INC - Document Type transform and Document Linking (Enrichment)"
     errInfo=f"Error  : {errorInfo}"
     fileid=f"File Header ID (TxM Staging) : {file_header_id}"
     billid=f"Bill ID : {bill_id}"
     src=f"Source : {source}"
     txminvoice=f"TMI Invoice Number : {txm_invoice_number}"
     time=f"Timestamp : {timestamp}"
     errLog=f"Fields & Content from Error log : {errormessage}"
     log_message=f"{process}\n{errInfo}\n{src}\n{fileid}\n{billid}\n{txminvoice}\n{time}"
     logger.error(log_message)
     subject=f"{env_profile} - {source_tp_str} - SOLUTIONS INC - {errorInfo} -{severity}"
     
     response=sns_client.publish(
        TopicArn=sns_topic_arn,
        Subject=subject,
        Message=f"{process}\n{errInfo}\n{src}\n{fileid}\n{billid}\n{time}\n{errLog}"
        )
     return response
     
def log_and_email_attachment(failed_bills_log_df,process,env_profile, file_header_id, batch_id, source, trading_partner, bucket_name, source_prefix, target_prefix, file_name):
    logger.info("inside log email attachment method")
    try:
        current_date = datetime.now().strftime('%Y-%m-%d')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        cst_time = datetime.now(pytz.timezone('America/Chicago')).strftime('%Y/%m/%d %H:%M:%SCST')
        source_prefix_with_date = f"{source_prefix}/{current_date}/"
        target_prefix_with_date = f"{target_prefix}/{current_date}/"
        logger.info(f'source_prefix_with_date : {source_prefix_with_date}')
        logger.info(f'target_prefix_with_date : {target_prefix_with_date}')

        failed_bills_error_log_df = failed_bills_log_df.withColumn('ErrorInfo',concat(lit("Validation Failed for Bill ID "), col('bill_header_id'),lit(" and Invoice Number "), col('txm_invoice_number'))) \
            .withColumn('Process',lit(process)) \
            .withColumnRenamed('batch_id','Error Log ID') \
            .withColumnRenamed('txm_invoice_number','Txm Invoice Number') \
            .withColumnRenamed('bill_header_id','Staging Bill ID') \
            .withColumnRenamed('unique_bill_id','Trading Partner Bill ID') \
            .withColumnRenamed('error_message','Error') \
            .withColumnRenamed('field_name_1','Field name 1') \
            .withColumnRenamed('field_value_1','Field Value 1') \
            .withColumnRenamed('field_name_2','Field Name 2') \
            .withColumnRenamed('field_value_2','Field Value 2') \
            .withColumnRenamed('update_timestamp','TimeStamp') \
            .select(col('Staging Bill ID'),col('Error Log ID'),col('Txm Invoice Number'),col('Trading Partner Bill ID'),col('Process'),col('ErrorInfo'),col('Error'),col('Field Name 1'),col('Field Value 1'),col('Field Name 2'),col('Field Value 2'),col('TimeStamp'))

        failed_bills_error_log_df.printSchema()
        failed_bills_error_log_df.show(10, truncate=False)
        recordCount = failed_bills_error_log_df.count()
        logger.info(f"Validation Error Mail Record Count => {recordCount}")

        failed_bills_error_log_df.coalesce(1).write.option('header','true').mode('append').format('csv').save(f"s3://{bucket_name}/{source_prefix_with_date}")
        source=source.upper()
        if trading_partner and trading_partner.strip() and trading_partner.lower() != "null":
            source_tp_str = trading_partner
            trading_partner=trading_partner.upper()
        else:
            source_tp_str = source
        source_tp_str = source_tp_str.upper()

        s3_client = boto3.client('s3','us-east-1')
        target_file_name = f'Error_log_{env_profile}_{source}_{trading_partner}_{file_header_id}_{timestamp}.csv'
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
                    'html': f"""<p>Please find the attached error log for {source_tp_str} with file header ID {file_header_id}.</p>
                                <p>Source: {source}</p>
                                <p>Trading partner: {trading_partner}</p>
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

## @params: [claimant-billingestion-document_load-job]
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'env_profile',
    's3_bucket',
    'source',
    'trading_partner',
    'file_header_id',
    's3_doc_files_prefix',
    'datacatalog_schema_name',
    'lambda_function_nodemailer',
    'datacatalog_doc_type_table_name',
    'datacatalog_com_doc_type_table_name',
    'db_secret_key',
    'sns_topic_arn',
    's3_error_attachment_prefix',
    'error_alerts_to_email_ids',
    'reprocess_flag'
])

#Create GlueContext, SparkContext, and SparkSession
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job=Job(glueContext)
job.init(args['JOB_NAME'],args)
logger.info("Glue job initialized successfully...")
job_id = args['JOB_RUN_ID']
logger.info(f"jobid-{job_id}")

env_profile=args['env_profile']
s3_bucket=args['s3_bucket']
source=args['source']
trading_partner=args['trading_partner']
file_header_id=args['file_header_id']
s3_doc_files_prefix=args['s3_doc_files_prefix']
datacatalog_schema_name=args['datacatalog_schema_name']
datacatalog_doc_type_table_name=args['datacatalog_doc_type_table_name']
datacatalog_com_doc_type_table_name=args['datacatalog_com_doc_type_table_name']
lambda_function_nodemailer = args['lambda_function_nodemailer']
db_secret_key=args['db_secret_key']
sns_topic_arn=args['sns_topic_arn']
s3_error_attachment_prefix=args['s3_error_attachment_prefix']
error_alerts_to_email_ids=args['error_alerts_to_email_ids']
reprocess_flag=args.get('reprocess_flag', 'N').upper()  # Default to N if not present

#Initializations
batch_id=None
is_transform_document_type = False
is_transform_document_type_error = False
is_document_linking = False
is_document_linking_error = False
is_skip_execution = False
is_skip_document_type_execution = False
is_skip_document_linking_execution = False
is_no_data_execution = False
file_name = ""
log_status = ""
status="COMPLETED"
mode = "append"

#Replacing trading partner in s3 prefix location
logger.info(f"s3_doc_files_prefix => {s3_doc_files_prefix}")
s3_doc_files_prefix = s3_doc_files_prefix.replace("<SOURCE>",source).replace("<TRADING_PARTNER>",trading_partner)
logger.info(f"NEW s3_doc_files_prefix => {s3_doc_files_prefix}")

spark.conf.set("spark.sql.ansi.enabled","True")

try:
    logger.info(f"Processing STARTED for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id}...")

    logger.info(f"db_secret_key => {db_secret_key}")
    secrets = get_secret(db_secret_key, 'us-east-1')

    db_sp_params = source,trading_partner, file_header_id, None, job_id, 'Glue - ETL', 'Glue - ETL', 0
    logger.info(f"db_sp_params => {db_sp_params}")
    dbStoredProcedure = "txm_bitx_enrichment_logs.insert_document_type_logs"
    logger.info(f"dbStoredProcedure => {dbStoredProcedure}")
    batch_id = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, 7)
    logger.info(f"batch_id => {batch_id}")

    # Read File Header Table to get the Bill File Name records from db with filtered file_header_id and trading_partner
    if trading_partner == 'jopari':
        read_db_sql = f"""
            SELECT sf.*
            FROM txm_bitx_staging.jopari_file_header_record_01 sf
            WHERE sf.file_header_id = {file_header_id}"""
        
    elif trading_partner == 'align':
        read_db_sql = f"""
            SELECT sf.*
            FROM txm_bitx_staging.align_file_header sf
            WHERE sf.file_header_id = {file_header_id}""" 
        
    elif trading_partner == 'techhealth':
        read_db_sql = f"""
            SELECT sf.*
            FROM txm_bitx_staging.techhealth_file_header sf
            WHERE sf.file_header_id = {file_header_id}"""

    file_header_df=get_df_db_data_with_query(read_db_sql, secrets)
    file_header_df=file_header_df.cache()
    file_header_df.printSchema()
    file_header_df.show(5, truncate=False)
    recordCount = file_header_df.count()
    logger.info(f"File Header Record Count => {recordCount}")

    if recordCount > 0:
        file_name = file_header_df.first()["file_name"]
        logger.info(f"file_name => {file_name}")
    else:
        is_no_data_execution = True
        logger.info(f"No Data Avaialble for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id}. Skipping the rest of the flow for document load!!!")
        #Error Flow
        error_message = f"CustomError: No Data Found in the Batch. Stopping further batch processing!!!"
        logger.error(f"error_message => {error_message}")
        raise Exception(error_message)

    logger.info(f"reprocess_flag => {reprocess_flag}")
    
    read_db_sql = f"""
        SELECT * FROM txm_bitx_enrichment_logs.batch_document_master 
        WHERE source = '{source}'
        AND (trading_partner IS NULL OR trading_partner = '{trading_partner}')
        AND file_header_id = {file_header_id}
        AND status <> 'FAILED DUPLICATE BATCH'
        AND batch_id <> {batch_id}
        ORDER BY start_datetime DESC, end_datetime DESC
        LIMIT 1"""

    existing_log_df=get_df_db_data_with_query(read_db_sql, secrets)
    existing_log_df=existing_log_df.cache()
    existing_log_df.printSchema()
    existing_log_df.show(5, truncate=False)
    existing_log_count = existing_log_df.count()
    logger.info(f"Existing Log status check Record Count => {existing_log_count}")
    
    # If entry exists, raise an error alert if not Reprocessing flow.
    if existing_log_count > 0:
        log_status = existing_log_df.first()["status"]
        logger.info(f'log_status : {log_status}')
        if reprocess_flag == 'Y':
            if log_status in ['COMPLETED','COMPLETED SKIP REPROCESS']:
                is_skip_execution = True
                logger.info(f"Existing entry found for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id} with status as {log_status} for reprocessing flow. No further processing required, skipping rest of the flow!!!")
            elif log_status == 'COMPLETED WITH DOCUMENT LINKING PROCESS ERROR':
                is_skip_document_type_execution = True
                logger.info(f"Existing entry found (Document Type) for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id} with status as {log_status} for reprocessing flow. No further processing required, skipping rest of the flow!!!")
            elif log_status == 'COMPLETED WITH DOCUMENT TYPE CODE MAPPING ERROR':
                is_skip_document_linking_execution = True
                logger.info(f"Existing entry found (Document Linking) for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id} with status as {log_status} for reprocessing flow. No further processing required, skipping rest of the flow!!!")
            else:
                logger.info(f"Existing entry found for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id} with status as {log_status} for reprocessing flow. Reprocessing flow started...")
        else:
            #Error Flow
            error_message = f"CustomError: Duplicate Batch, Log entry already exists for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id} with reprocess_flag as {reprocess_flag} (not as Y)."
            logger.error(f"error_message => {error_message}")
            raise Exception(error_message)
    else:
        logger.info(f"No existing entry found for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id}. Proceeding with document load...")
    
    if not is_skip_execution:
        if not is_skip_document_type_execution:
            if trading_partner == 'jopari':
                # Read from catalog / enhanced db
                read_db_sql = f"""
                SELECT h.file_header_id, h.bill_header_id, h.trading_partner, h.bill_id, h.txm_invoice_number, d.billing_documents_id, d.document_type_code, sf.file_name
                FROM txm_bitx.bill_header h
                JOIN txm_bitx.billing_documents d ON h.bill_id = d.bill_id AND d.file_type = 'ATTACHMENT'
                JOIN txm_bitx_staging.jopari_file_header_record_01 sf ON h.file_header_id = sf.file_header_id
                WHERE h.source = '{source}'
                AND h.trading_partner = '{trading_partner}'
                AND h.file_header_id = {file_header_id}
                """
            elif trading_partner in ('align','techhealth'):
                read_db_sql = f"""
                SELECT h.file_header_id, h.bill_header_id, h.trading_partner, h.bill_id, h.txm_invoice_number, d.billing_documents_id, d.document_type_code, sf.file_name
                FROM txm_bitx.bill_header h
                JOIN txm_bitx.billing_documents d ON h.bill_id = d.bill_id AND d.file_type = 'ATTACHMENT'
                JOIN txm_bitx_staging.{trading_partner}_file_header sf ON h.file_header_id = sf.file_header_id
                WHERE h.source = '{source}'
                AND h.trading_partner = '{trading_partner}'
                AND h.file_header_id = {file_header_id}
                """

            # Reading trading partner doctype df 
            tp_doc_type_df=get_df_db_data_with_query(read_db_sql, secrets)
            tp_doc_type_df=tp_doc_type_df.cache()
            tp_doc_type_df.printSchema()
            tp_doc_type_df.show(5, truncate=False)
            tp_doc_type_count = tp_doc_type_df.count()
            logger.info(f"File Header Record Count => {tp_doc_type_count}")

            if tp_doc_type_count > 0:
                is_transform_document_type = True
                is_document_linking = False
                logger.info(f"Transform Document Type Code process STARTED for Source {source} trading partner {trading_partner} and File Header ID {file_header_id}...")
                # Reading mapping configuration from catalog
                if trading_partner == 'jopari':
                    logger.info(f"Reading Bill Ingestion jopari doctype mapping configuration from Data Catalog table => {datacatalog_schema_name}.{datacatalog_doc_type_table_name}...")

                    doctype_map_frame = glueContext.create_dynamic_frame.from_catalog(database=datacatalog_schema_name, table_name=datacatalog_doc_type_table_name, transformation_ctx="doctype_map_frame")
                    doctype_map_frame.printSchema()
                    doctype_map_frame.show()
                    doctype_map_df = doctype_map_frame.toDF()
                    doctype_map_df.printSchema()
                    doctype_map_df.show(5, truncate=False)
                    recordCount = doctype_map_df.count()
                    logger.info(f"Doc Type Mapping Record Count => {recordCount}")

                    # Map tp doc type based on mapping table
                    # Scenario1
                    txm_doc_type_df = tp_doc_type_df.join(doctype_map_df, tp_doc_type_df["document_type_code"] == doctype_map_df["tp_doc_type_code"], how="left")
                    txm_doc_type_df.printSchema()
                    txm_doc_type_df.show(5, truncate=False)
                    recordCount = txm_doc_type_df.count()
                    logger.info(f"TxM Doc Type - 1 Record Count => {recordCount}")

                    # Scenario2
                    txm_doc_type_df = txm_doc_type_df.withColumn("txm_doc_type", when(col("txm_doc_type").isNotNull(), col("txm_doc_type")).otherwise("MEDNAR"))
                    txm_doc_type_df.printSchema()
                    txm_doc_type_df.show(5, truncate=False)
                    recordCount = txm_doc_type_df.count()
                    logger.info(f"TxM Doc Type - 2 Record Count => {recordCount}")
                        
                    # Updating database table with txm_doc_type 
                    update_main_table_name = f"txm_bitx.billing_documents"
                    logger.info(f"update_main_table_name => {update_main_table_name}")

                    update_stage_table_name = f"{update_main_table_name}_{trading_partner}_{file_header_id}_stage"
                    logger.info(f"update_stage_table_name => {update_stage_table_name}")
                    v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

                    update_db_sql = f"""
                        UPDATE {update_main_table_name} main
                        JOIN {update_stage_table_name} stage ON main.billing_documents_id = stage.billing_documents_id
                        SET main.txm_doc_type = stage.txm_doc_type,
                        main.update_user = 'Glue - ETL - Document loading',
                        main.update_timestamp = '{v_load_timestamp}'
                    """

                    #Calling update_db_records 
                    update_db_records(txm_doc_type_df, secrets, update_main_table_name, update_stage_table_name, update_db_sql)
                    
                    # Scenario 3
                    # Count the occurrences of each d_bill_id with bill_image as "YES"
                    bill_image_counts_df = txm_doc_type_df.groupBy("bill_id").agg(count(expr("CASE WHEN bill_image = 'YES' THEN 1 END")).alias("bill_image_count"))
                    bill_image_counts_df.printSchema()
                    bill_image_counts_df.show(5, truncate=False)
                    recordCount = bill_image_counts_df.count()
                    logger.info(f"Bill Image Count Record Count => {recordCount}")

                    # Join the aggregated results back to the original dataframe to retain all columns
                    bill_image_counts_df = txm_doc_type_df.join(bill_image_counts_df, "bill_id", "inner")
                    bill_image_counts_df.show(5, truncate= False)
                    recordCount = bill_image_counts_df.count()
                    logger.info(f"Bill Image after retain all columns  Count Record Count => {recordCount}")

                    # Filter bill_ids where at least one bill_image is "YES"
                    passed_bills_df = bill_image_counts_df.filter(col("bill_image_count") > 0)
                    passed_bills_df.printSchema()
                    passed_bills_df.show(5, truncate=False)
                    passed_bills_count = passed_bills_df.count()
                    logger.info(f"Bill Image PASSED Count Record Count => {passed_bills_count}")

                    # # Filter d_bill_ids where none of the bill_image are "YES"
                    failed_bills_df = bill_image_counts_df.filter(col("bill_image_count") == 0)
                    failed_bills_df.printSchema()
                    failed_bills_df.show(5, truncate=False)
                    failed_bills_df_count = failed_bills_df.count()
                    logger.info(f"Bill Image FAILED Count Record Count => {failed_bills_df_count}")

                    failed_bills_txm_id_df = failed_bills_df.select("txm_invoice_number").distinct()
                    failed_bills_txm_id_df.printSchema()
                    failed_bills_txm_id_df.show(5, truncate=False)
                    failed_bills_txm_id_count = failed_bills_txm_id_df.count()
                    logger.info(f"Missing Records Count => {failed_bills_txm_id_count}")

                    if failed_bills_txm_id_count > 0:
                        logger.info(f"Found Bill_ids with no bill_image...")
                        invoice_status_type='INDICATED_ATTACHMENT_MISSING'
                        invoice_status='MARKED_FOR_DELETION'
                        v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                        
                        update_invoice_status(failed_bills_txm_id_df, secrets, invoice_status_type, invoice_status)
                        
                        failed_bills_log_df=failed_bills_df \
                            .withColumn('batch_id',lit(batch_id)) \
                            .withColumn('error_code',lit('Glue')) \
                            .withColumn('error_type',lit("Bill Image Validation")) \
                            .withColumn('error_message',lit('Bill Image Does Not Exist')) \
                            .withColumn('create_user',lit('Glue - ETL')) \
                            .withColumn('update_user',lit('Glue - ETL')) \
                            .withColumn('create_timestamp',lit(v_load_timestamp)) \
                            .withColumn('update_timestamp',lit(v_load_timestamp)) \
                            .select(col("batch_id"),col("bill_id"),col("txm_invoice_number"),col("error_code"),col("error_type"),col("error_message"),col("create_user"),col("update_user"),col("create_timestamp"),col("update_timestamp"))
                        failed_bills_log_df.printSchema()
                        failed_bills_log_df.show(5, truncate=False)
                        recordCount = failed_bills_log_df.count()
                        logger.info(f"Failed Bills Logs DF result Record Count => {recordCount}")

                        log_error_table_db="txm_bitx_enrichment_logs.batch_document_error_detail"
                        logger.info(f'Load log_error_table_db dataframe to RDS table => {log_error_table_db}')
                        load_data_to_rds(failed_bills_log_df, log_error_table_db, secrets, mode)

                        read_db_sql = f"""SELECT DISTINCT e.batch_id
                                                ,b.txm_invoice_number
                                                ,f.bill_header_id
                                                ,b.unique_bill_id
                                                ,CONVERT(e.error_message USING utf8) AS error_message
                                                ,'billing_documents.document_type_code' AS field_name_1
                                                ,d.document_type_code AS field_value_1
                                                ,'billing_documents.txm_doc_type' AS field_name_2
                                                ,d.txm_doc_type AS field_value_2
                                                ,DATE_FORMAT(CONVERT_TZ(IFNULL(e.update_timestamp, e.create_timestamp), 'UTC', 'America/Chicago'), '%Y/%m/%d %H:%i:%sCST') AS update_timestamp
                                            FROM txm_bitx.bill_header b
                                            JOIN txm_bitx.billing_documents d ON b.bill_id = d.bill_id
                                            JOIN txm_bitx_enrichment_logs.batch_document_error_detail e ON b.bill_id = e.bill_id
                                            JOIN txm_bitx_staging.jopari_bill_header_record_10 f ON b.bill_header_id = f.bill_header_id
                                            WHERE b.source = '{source}'
                                            AND b.trading_partner = '{trading_partner}' 
                                            AND b.file_header_id = {file_header_id}
                                            AND e.batch_id = {batch_id}
                                            AND e.error_type = 'Bill Image Validation'
                                    """
                        
                        error_log_df = get_df_db_data_with_query(read_db_sql, secrets)
                        error_log_df = error_log_df.cache()
                        error_log_df.printSchema()
                        error_log_df.show(5, truncate=False)
                        error_count = error_log_df.count()
                        logger.info(f"Error Log Record Count => {error_count}")

                        if error_count > 0:
                            is_transform_document_type_error = True
                            logger.info(f"Found Bill Image Validation Errors. Sending Consolidated Error mails with error details with attach...")
                            process='eBill Validations'
                            response = log_and_email_attachment(error_log_df,process,env_profile, file_header_id, batch_id, source, trading_partner, s3_bucket, s3_error_attachment_prefix, s3_error_attachment_prefix,file_name)
                            logger.info(f"ErrorLog File Created successfully: {response}")

                            if passed_bills_count > 0:
                                logger.info("Found Validation PASSed Bills in the Batch. Proceeding for further batch processing...")
                            else:
                                #Error Flow
                                error_message = f"CustomError: All Bills Validation FAILED in the Batch. Stopping further batch processing!!!"
                                logger.error(f"error_message => {error_message}")
                                raise Exception(error_message)
                        else:
                            logger.info("No Bill Image Validation Errors found in the log table!!!")
                        
                    else:
                        logger.info(f"No Bill Image Validation Errors found in the Batch!!!")

                elif trading_partner in ('techhealth','align'):
                    logger.info(f"Reading Bill Ingestion common doctype mapping configuration from Data Catalog table => {datacatalog_schema_name}.{datacatalog_com_doc_type_table_name}...")

                    doctype_map_frame = glueContext.create_dynamic_frame.from_catalog(database=datacatalog_schema_name, table_name=datacatalog_com_doc_type_table_name, transformation_ctx="doctype_map_frame")
                    doctype_map_frame.printSchema()
                    doctype_map_frame.show()
                    doctype_map_df = doctype_map_frame.toDF()
                    doctype_map_df.printSchema()
                    doctype_map_df.show(5, truncate=False)
                    recordCount = doctype_map_df.count()
                    logger.info(f"Doc Type Mapping Record Count => {recordCount}")

                    txm_doc_type_df = tp_doc_type_df.join(doctype_map_df, tp_doc_type_df["document_type_code"] == doctype_map_df["tp_doc_type_code"], how="left")
                    txm_doc_type_df.printSchema()
                    txm_doc_type_df.show(5, truncate=False)
                    recordCount = txm_doc_type_df.count()
                    logger.info(f"TxM Doc Type - 1 Record Count => {recordCount}")

                    # Updating database table with txm_doc_type 
                    update_main_table_name = f"txm_bitx.billing_documents"
                    logger.info(f"update_main_table_name => {update_main_table_name}")

                    update_stage_table_name = f"{update_main_table_name}_{trading_partner}_{file_header_id}_stage"
                    logger.info(f"update_stage_table_name => {update_stage_table_name}")
                    v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

                    update_db_sql = f"""
                        UPDATE {update_main_table_name} main
                        JOIN {update_stage_table_name} stage ON main.billing_documents_id = stage.billing_documents_id
                        SET main.txm_doc_type = stage.txm_doc_type,
                        main.update_user = 'Glue - ETL - Document loading',
                        main.update_timestamp = '{v_load_timestamp}'
                    """

                    #Calling update_db_records 
                    update_db_records(txm_doc_type_df, secrets, update_main_table_name, update_stage_table_name, update_db_sql)
                
                #updated log table
                db_sp_params = batch_id, None, None, 'TRANFORM DOCUMENT TYPE CODE COMPLETED', 'Glue', None, None, 'Glue - ETL', 'Glue - ETL'
                logger.info(f"db_sp_params => {db_sp_params}")
                dbStoredProcedure = "txm_bitx_enrichment_logs.update_document_type_log_status"
                logger.info(f"dbStoredProcedure => {dbStoredProcedure}")
                res = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, None)
                logger.info(f"res => {res}")
                
                logger.info(f"Transform Document Type Code process COMPLETED for Source {source} trading partner {trading_partner} and File Header ID {file_header_id}!!!")
            else:
                if trading_partner == 'jopari':
                    is_no_data_execution = True
                    logger.info(f"No Data Avaialble for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id}. Skipping the rest of the flow for document type!!!")
                    #Error Flow
                    error_message = f"CustomError: No Data Found in the Batch. Stopping further batch processing!!!"
                    logger.error(f"error_message => {error_message}")
                    raise Exception(error_message)
                else:
                    logger.info(f"No attachments Avaialble for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id}. further processing the steps")
        else:
            logger.info(f"Existing entry found (Document Type) for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id} with status as {log_status} for reprocessing flow. No further processing required, skipping rest of the flow!!!")
        
        if not is_skip_document_linking_execution:
            is_transform_document_type = False
            is_document_linking = True
            logger.info(f"Document Linking process STARTED for Source {source} trading partner {trading_partner} and File Header ID {file_header_id}...")

            read_db_sql = f"""
                SELECT b.bill_id, b.trading_partner, b.file_header_id, b.unique_bill_id, b.txm_invoice_number, bd.billing_documents_id, bd.attachment_control_number
                FROM txm_bitx.bill_header b
                JOIN txm_bitx.billing_documents bd ON b.bill_id = bd.bill_id
                WHERE b.source = '{source}'
                AND b.trading_partner = '{trading_partner}' 
                AND b.file_header_id = {file_header_id}
                AND bd.attachment_control_number IS NOT NULL
                AND bd.attachment_control_number <> ''"""

            billing_documents_df=get_df_db_data_with_query(read_db_sql, secrets)
            billing_documents_df=billing_documents_df.cache()
            billing_documents_df.printSchema()
            billing_documents_df.show(5, truncate=False)
            bill_documents_count = billing_documents_df.count()
            logger.info(f"Billing Documents Record Count => {bill_documents_count}")

            billing_documents_txm_id_df=billing_documents_df.select("txm_invoice_number").distinct()
            billing_documents_txm_id_df.printSchema()
            billing_documents_txm_id_df.show(5, truncate=False)
            billing_documents_txm_id_count = billing_documents_txm_id_df.count()
            logger.info(f"Billing Documents txm_invoice_number Record Count => {billing_documents_txm_id_count}")

            bill_doc_folder_name = file_name.split('.', 1)[0]
            # Replace "Bills" with "Images" if it exists
            if trading_partner == 'jopari':                
                bill_doc_folder_name = bill_doc_folder_name.replace("Bills","Images")

            logger.info(f"bill_doc_folder_name => {bill_doc_folder_name}")
            
            s3_bill_doc_folder_prefix=s3_doc_files_prefix + bill_doc_folder_name + "/"
            logger.info(f"s3_bill_doc_folder_prefix => {s3_bill_doc_folder_prefix}")
            s3_bill_doc_list = get_s3_objects(bucket = s3_bucket, prefix = s3_bill_doc_folder_prefix, suffix = None)
            logger.info(f"s3_bill_doc_list => {s3_bill_doc_list}")

            if s3_bill_doc_list:
                s3_bill_docs_df=spark.createDataFrame(s3_bill_doc_list, "string").toDF("s3_bill_doc_path")
                s3_bill_docs_df=s3_bill_docs_df.withColumn('s3_bill_doc_path',concat(lit(f"s3://{s3_bucket}/"),col("s3_bill_doc_path")))
                s3_bill_docs_df.printSchema()
                s3_bill_docs_df.show(5, truncate=False)
                recordCount = s3_bill_docs_df.count()
                logger.info(f"S3 Bill Docs Record Count => {recordCount}")

            else:
                logger.info(f"No Attachments found in S3 folder {s3_bill_doc_folder_prefix}.")
            
            if billing_documents_txm_id_count > 0:
                if not s3_bill_doc_list:
                    #Error Flow
                    error_message = f"CustomError: No Attachments found in S3 folder {s3_bill_doc_folder_prefix}. Stopping further batch processing!!!"
                    logger.error(f"error_message => {error_message}")
                    raise Exception(error_message)

                update_billing_documents_df=billing_documents_df.join(s3_bill_docs_df, col('s3_bill_doc_path').contains(col('attachment_control_number')), 'inner')
                update_billing_documents_df.printSchema()
                update_billing_documents_df.show(5, truncate=False)
                recordCount = update_billing_documents_df.count()
                logger.info(f"Update Billing Documents Record Count => {recordCount}")

                update_main_table_name = f"txm_bitx.billing_documents"
                logger.info(f"update_main_table_name => {update_main_table_name}")

                update_stage_table_name = f"{update_main_table_name}_{trading_partner}_{file_header_id}_stage"
                logger.info(f"update_stage_table_name => {update_stage_table_name}")
                v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

                update_db_sql = f"""
                    UPDATE txm_bitx.billing_documents bd 
                    JOIN {update_stage_table_name} sbd ON bd.billing_documents_id = sbd.billing_documents_id
                    SET bd.document_path = sbd.s3_bill_doc_path,
                    bd.update_user = 'Glue - ETL - Document loading',
                    bd.update_timestamp = '{v_load_timestamp}'
                """

                update_db_records(update_billing_documents_df, secrets, update_main_table_name, update_stage_table_name, update_db_sql)

                # Find missing records
                join_condition = [billing_documents_df['attachment_control_number'] == update_billing_documents_df['attachment_control_number']]
                non_matching_records_df = billing_documents_df.join(update_billing_documents_df, join_condition, 'left_anti')
                non_matching_records_df.printSchema()
                non_matching_records_df.show(5, truncate=False)
                non_matching_count = non_matching_records_df.count()
                logger.info(f"Missing Records Count => {non_matching_count}")

                non_matching_txm_id_df = non_matching_records_df.select("txm_invoice_number").distinct()
                non_matching_txm_id_df.printSchema()
                non_matching_txm_id_df.show(5, truncate=False)
                non_matching_txm_id_count = non_matching_txm_id_df.count()
                logger.info(f"Missing Records Distinct Count => {non_matching_txm_id_count}")

                if non_matching_txm_id_count > 0:
                    #email part
                    logger.info(f"Found Bill_ids with no bill_image attachments...")
                    logger.info(f"Update invoice_status table...")
                    invoice_status_type='INDICATED_ATTACHMENT_MISSING'
                    invoice_status='MARKED_FOR_DELETION'
                    v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                    
                    update_invoice_status(non_matching_txm_id_df, secrets, invoice_status_type, invoice_status)

                    failed_bills_log_df=non_matching_records_df \
                        .withColumn('batch_id',lit(batch_id)) \
                        .withColumn('error_code',lit('Glue')) \
                        .withColumn('error_type',lit("Bill Attachment Validation")) \
                        .withColumn('error_message',lit('Attachment expected but not found')) \
                        .withColumn('create_user',lit('Glue - ETL')) \
                        .withColumn('update_user',lit('Glue - ETL')) \
                        .withColumn('create_timestamp',lit(v_load_timestamp)) \
                        .withColumn('update_timestamp',lit(v_load_timestamp)) \
                        .select(col("batch_id"),col("bill_id"),col("txm_invoice_number"),col("error_code"),col("error_type"),col("error_message"),col("create_user"),col("update_user"),col("create_timestamp"),col("update_timestamp"))
                    failed_bills_log_df.printSchema()
                    failed_bills_log_df.show(5, truncate=False)
                    recordCount = failed_bills_log_df.count()
                    logger.info(f"Failed Bills Logs DF result Record Count => {recordCount}")

                    log_error_table_db="txm_bitx_enrichment_logs.batch_document_error_detail"
                    logger.info(f'Load log_error_table_db dataframe to RDS table => {log_error_table_db}')
                    load_data_to_rds(failed_bills_log_df, log_error_table_db, secrets, mode)

                    read_db_sql = f"""SELECT DISTINCT e.batch_id
                                            ,b.txm_invoice_number
                                            ,b.bill_header_id
                                            ,CASE WHEN b.trading_partner = 'jopari' THEN b.unique_bill_id ELSE b.invoice_id END AS unique_bill_id
                                            ,CONVERT(e.error_message USING utf8) AS error_message
                                            ,'billing_documents.attachment_control_number' AS field_name_1
                                            ,d.attachment_control_number AS field_value_1
                                            ,'' AS field_name_2
                                            ,'' AS field_value_2
                                            ,DATE_FORMAT(CONVERT_TZ(IFNULL(e.update_timestamp, e.create_timestamp), 'UTC', 'America/Chicago'), '%Y/%m/%d %H:%i:%sCST') AS update_timestamp
                                        FROM txm_bitx.bill_header b
                                        JOIN txm_bitx.billing_documents d ON b.bill_id = d.bill_id
                                        JOIN txm_bitx_enrichment_logs.batch_document_error_detail e ON b.bill_id = e.bill_id
                                        WHERE b.source = '{source}'
                                        AND b.trading_partner = '{trading_partner}' 
                                        AND b.file_header_id = {file_header_id}
                                        AND e.batch_id = {batch_id}
                                        AND e.error_type = 'Bill Attachment Validation'
                                        AND d.attachment_control_number IS NOT NULL
                                        AND d.attachment_control_number <> ''
                                        AND d.document_path IS NULL
                                """
                        
                    error_log_df = get_df_db_data_with_query(read_db_sql, secrets)
                    error_log_df = error_log_df.cache()
                    error_log_df.printSchema()
                    error_log_df.show(5, truncate=False)
                    error_count = error_log_df.count()
                    logger.info(f"Error Log Record Count => {error_count}")

                    if error_count > 0:
                        is_document_linking_error = True
                        logger.info(f"Found Bill Image Attachment Errors. Sending Consolidated Error mails with error details with attach...")
                        process='eBill Validations'
                        response = log_and_email_attachment(error_log_df,process, env_profile, file_header_id, batch_id, source, trading_partner, s3_bucket, s3_error_attachment_prefix, s3_error_attachment_prefix, file_name)
                        logger.info(f"ErrorLog File Created successfully: {response}")

                        if billing_documents_txm_id_count == non_matching_txm_id_count:
                            #Error Flow
                            error_message = f"CustomError: All Bills Validation FAILED in the Batch. Stopping further batch processing!!!"
                            logger.error(f"error_message => {error_message}")
                            raise Exception(error_message)
                        else:
                            logger.info("Found Validation PASSed Bills in the Batch. Proceeding for further batch processing...")
                    else:
                        logger.info("No Bill Image Attachment Errors found in the log table!!!")

                else:
                    logger.info(f"No Bill Image Validation Errors found in the Batch!!!")

            else:
                # This Scenario not possible for Jopari because it would have caught in Document Type convertion step. For Align and Techhealth attachments are optional.
                is_no_data_execution = True
                logger.info(f"No Data Avaialble for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id}. Skipping the rest of the flow for document linking!!!") 

            if s3_bill_doc_list:
                logger.info(f"Started Orphan attachments flow...")

                if trading_partner == 'jopari':
                    
                    read_db_sql = f"""
                        SELECT b.bill_header_id, b.file_header_id, b.unique_bill_id,bd.attachment_control_number
                        FROM txm_bitx_staging.jopari_bill_header_record_10 b
                        JOIN txm_bitx_staging.jopari_attachments_record_40 bd ON b.bill_header_id = bd.bill_header_id
                        WHERE b.file_header_id = {file_header_id}
                        AND bd.attachment_control_number IS NOT NULL
                        AND bd.attachment_control_number <> ''"""
                
                elif trading_partner == 'align':

                    read_db_sql = f"""
                        SELECT b.invoice_header_id AS bill_header_id, b.file_header_id, b.invoice_number AS unique_bill_id, bd.document_id AS attachment_control_number
                        FROM txm_bitx_staging.align_invoice_header b
                        JOIN txm_bitx_staging.align_attachment bd ON b.invoice_header_id = bd.invoice_header_id
                        WHERE b.file_header_id = {file_header_id}
                        AND bd.document_id IS NOT NULL
                        AND bd.document_id <> ''"""

                elif trading_partner == 'techhealth':

                    read_db_sql = f"""
                        SELECT b.invoice_header_id AS bill_header_id, b.file_header_id, b.invoice_number AS unique_bill_id, bd.document_id AS attachment_control_number
                        FROM txm_bitx_staging.techhealth_invoice_header b
                        JOIN txm_bitx_staging.techhealth_attachment bd ON b.invoice_header_id = bd.invoice_header_id
                        WHERE b.file_header_id = {file_header_id}
                        AND bd.document_id IS NOT NULL
                        AND bd.document_id <> ''"""

                logger.info(f"read_db_sql for staging =>{read_db_sql}")
                stg_billing_documents_df=get_df_db_data_with_query(read_db_sql, secrets)
                stg_billing_documents_df=stg_billing_documents_df.cache()
                stg_billing_documents_df.printSchema()
                stg_billing_documents_df.show(5, truncate=False)
                stg_bill_documents_count = stg_billing_documents_df.count()
                logger.info(f"Staging Billing Documents Record Count for {source} & {trading_partner} => {stg_bill_documents_count}")
                
                
                # Find orphan attachments (find those attachments from s3 who dont have bill_id records in db)
                logger.info("Finding orphan attachments using join with contains...")
                orphan_attachments_df = s3_bill_docs_df.join(stg_billing_documents_df,s3_bill_docs_df['s3_bill_doc_path'].contains(stg_billing_documents_df['attachment_control_number']),'left')
                orphan_attachments_df = orphan_attachments_df.filter(orphan_attachments_df['attachment_control_number'].isNull())
                orphan_attachments_df.printSchema()
                orphan_attachments_df.show(5, truncate=False)
                orphan_attachments_count = orphan_attachments_df.count()
                logger.info(f"Orphan Attachments Count => {orphan_attachments_count}")

                # Sent email for orphan attachments
                if orphan_attachments_count > 0:
                # Orphan attachments found, take necessary action like sending an email or logging
                    logger.info(f"Found {orphan_attachments_count} orphan attachments.")
                    orphan_attachments_log_df=orphan_attachments_df \
                        .withColumn('batch_id',lit(batch_id)) \
                        .withColumn('error_code',lit('Glue')) \
                        .withColumn('error_type',lit("Orphan attachments")) \
                        .withColumn('error_message',lit('Invoice number not found for the Bill Image document')) \
                        .withColumn('create_user',lit('Glue - ETL')) \
                        .withColumn('update_user',lit('Glue - ETL')) \
                        .withColumn('create_timestamp',lit(v_load_timestamp)) \
                        .withColumn('update_timestamp',lit(v_load_timestamp)) \
                        .select(col("batch_id"),col("error_code"),col("error_type"),col("error_message"),col("create_user"),col("update_user"),col("create_timestamp"),col("update_timestamp"),col("s3_bill_doc_path"))
                    orphan_attachments_log_df.printSchema()
                    orphan_attachments_log_df.show(5, truncate=False)
                    recordCount = orphan_attachments_log_df.count()
                    logger.info(f"Orphan attachments Logs DF result Record Count => {recordCount}")
                    log_error_table_db="txm_bitx_enrichment_logs.batch_document_error_detail"
                    logger.info(f'Load log_error_table_db dataframe to RDS table => {log_error_table_db} & mode : {mode}')
                    # load in error log table
                    load_data_to_rds(orphan_attachments_log_df.drop('s3_bill_doc_path'), log_error_table_db, secrets, mode)
                    
                    # creating df according to email method 
                    cst_time = datetime.now(pytz.timezone('America/Chicago')).strftime('%Y/%m/%d %H:%M:%SCST')
                    orphan_error_log_df=orphan_attachments_log_df \
                        .withColumn('bill_header_id',lit('')) \
                        .withColumn('txm_invoice_number',lit(None).cast(DecimalType(10, 2))) \
                        .withColumn('unique_bill_id',lit("")) \
                        .withColumn('field_name_1',lit('Bill Image Document')) \
                        .withColumn("field_value_1",regexp_extract(col("s3_bill_doc_path"), r'([^/]+)$', 0)) \
                        .withColumn('field_name_2',lit("Bill Image Document Path")) \
                        .withColumn('field_value_2',col("s3_bill_doc_path")) \
                        .withColumn('update_timestamp',lit(cst_time))
                        
                    orphan_error_log_df.printSchema()
                    orphan_error_log_df.show(5, truncate=False)
                    recordCount = orphan_error_log_df.count()
                    logger.info(f"Orphan attachments error Logs DF result Record Count => {recordCount}")
                    
                    process='Orphan attachments'
                    logger.info(f"Sending email for Orphan attachments errors.....")
                    response = log_and_email_attachment(orphan_error_log_df,process, env_profile, file_header_id, batch_id, source, trading_partner, s3_bucket, s3_error_attachment_prefix, s3_error_attachment_prefix, file_name)
                    logger.info(f"ErrorLog File Created successfully for Orphan attachments: {response}") 
                else:
                    logger.info(f"No Orphan Attachments found in S3 folder {s3_bill_doc_folder_prefix} for the batch!!!")
            else:
                logger.info(f"No Attachments found in S3 folder {s3_bill_doc_folder_prefix}. Skipping Orphan attachments flow!!!")
        
            #updated log table
            db_sp_params = batch_id, None, None, 'DOCUMENT LINKING COMPLETED', 'Glue', None, None, 'Glue - ETL', 'Glue - ETL'
            logger.info(f"db_sp_params => {db_sp_params}")
            dbStoredProcedure = "txm_bitx_enrichment_logs.update_document_type_log_status"
            logger.info(f"dbStoredProcedure => {dbStoredProcedure}")
            res = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, None)
            logger.info(f"res => {res}")

            logger.info(f"Document Linking process COMPLETED for Source {source} trading partner {trading_partner} and File Header ID {file_header_id}!!!")
        else:
            logger.info(f"Existing entry found (Document Linking) for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id} with status as {log_status} for reprocessing flow. No further processing required, skipping rest of the flow!!!")

    else:
        logger.info(f"Existing entry found for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id} with status as {log_status} for reprocessing flow. No further processing required, skipping rest of the flow!!!")
    
    #Data for log table
    if is_transform_document_type_error and is_document_linking_error:
        status="COMPLETED WITH ERRORS"
    if is_transform_document_type_error:
        status="COMPLETED WITH DOCUMENT TYPE CODE MAPPING ERROR"
    elif is_document_linking_error:
        status="COMPLETED WITH DOCUMENT LINKING PROCESS ERROR"
    elif is_skip_execution:
        status="COMPLETED SKIP REPROCESS"
    elif is_no_data_execution:
        # This Scenario not possible for Jopari because it would have caught in Document Type convertion step. For Align, Techhealth and Optum attachments are optional.
        status="COMPLETED WITH NO DATA"
    else:
        status="COMPLETED"
    errorInfo="None"
    errormessage="None"
    logger.info(f"Processing COMPLETED for Source {source} trading partner {trading_partner} and File Header ID {file_header_id}!!!")
    
except Exception as e:
    if is_transform_document_type:
        status="FAILED IN TRANSFORM DOCUMENT TYPE CODE"
    elif is_document_linking:
        status="FAILED IN DOCUMENT LINKING"
    else:
        status="FAILED"
    
    severity="FATAL"

    if "CustomError: Duplicate Batch" in str(e):
        status="FAILED DUPLICATE BATCH"
        errorInfo="Duplicate Batch"
    elif "CustomError: No Data Found in the Batch." in str(e):
        status="FAILED NO DATA"
        errorInfo="No Data found in the Batch"
    elif "CustomError: No Attachments found in S3 folder" in str(e):
        status="FAILED NO ATTACHMENTS IN S3"
        errorInfo="No Attachments found in S3 folder"
    elif "CustomError: All Bills Validation FAILED in the Batch" in str(e):
        status="FAILED ALL BILLS"
        errorInfo="All Bills Validation FAILED in the Batch"
    else:
        status="FAILED"
        errorInfo="Unable to process 1 or more Bill(s)"
    logger.info("error in exception block")
    logger.info(errorInfo)  
    logger.error(str(e))
    #errormessage=re.search(r"(.+?Exception:.+?)(?=\n|$)",str(e)).group(1)
    errormessage=str(e)[:500]
    response=log_and_email(env_profile,errorInfo,file_header_id,None,None,source,trading_partner,errormessage,severity,sns_topic_arn)
    logger.info(f"Email sent successfully!message_id:{response['MessageId']}")
    raise
finally:
    #updated log table
    db_sp_params = batch_id, None, None, status, 'Glue', errorInfo, errormessage, 'Glue - ETL', 'Glue - ETL'
    logger.info(f"db_sp_params => {db_sp_params}")
    dbStoredProcedure = "txm_bitx_enrichment_logs.update_document_type_log_status"
    logger.info(f"dbStoredProcedure => {dbStoredProcedure}")
    res = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, None)
    logger.info(f"res => {res}")
try:
    #Commit the job
    job.commit()
    logger.info("Job committed successfully")
except Exception as e:
    logger.error(f"Error committing the job:{e}")
    raise