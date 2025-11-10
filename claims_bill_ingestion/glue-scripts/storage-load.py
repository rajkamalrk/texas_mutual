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


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("claimant-billingestion-storage-load-job")

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
    
def run_sql_query(db_sql,secrets):
    connection = pymysql.connect( 
        host = secrets["host"],
        database = secrets["dbClusterIdentifier"],
        port = secrets["port"],
        user = secrets["username"],
        password = secrets["password"],
        connect_timeout = 300
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute(db_sql)
            connection.commit()
    finally:
        connection.close()

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
            port=port,
            connect_timeout = 300
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
        ,main.update_user = 'Glue - ETL - Storage Load'
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
        SELECT s.status_id, '{status_type}', '{status}', '{status}', 'Glue - ETL - Storage Load', 'Glue - ETL - Storage Load', '{v_load_timestamp}', '{v_load_timestamp}'
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

def log_and_email(env_profile,errorInfo,file_header_id,source,trading_partner,batch_id,errormessage,severity,sns_topic_arn):
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
     process=f"Process : {source_tp_str} - Storage Load/Bill Image Creation (Enrichment)"
     errInfo=f"Error  : {errorInfo}"
     src=f"Source : {source}"
     fileid=f"File Header ID (TxM Staging) : {file_header_id}"
     logbatchid=f"Log Batch ID : {batch_id}"
     time=f"Timestamp : {timestamp}"
     errLog=f"Fields & Content from Error log : {errormessage}"
     log_message=f"{process}\n{errInfo}\n{src}\n{fileid}\n{logbatchid}\n{time}"
     logger.error(log_message)
     subject=f"{env_profile} - {source_tp_str} - {errorInfo} - {severity}"
     
     response=sns_client.publish(
        TopicArn=sns_topic_arn,
        Subject=subject,
        Message=f"{process}\n{errInfo}\n{src}\n{fileid}\n{logbatchid}\n{time}\n{errLog}"
        )
     return response

def log_and_email_attachment(error_df, process, env_profile, file_header_id, source, trading_partner, batch_id, bucket_name, source_prefix, target_prefix):
    logger.info("inside log email attachment method")
    try:
        current_date = datetime.now().strftime('%Y-%m-%d')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        cst_time = datetime.now(pytz.timezone('America/Chicago')).strftime('%Y/%m/%d %H:%M:%SCST')
        source_prefix_with_date = f"{source_prefix}/{current_date}/"
        target_prefix_with_date = f"{target_prefix}/{current_date}/"
        logger.info(f'source_prefix_with_date : {source_prefix_with_date}')
        logger.info(f'target_prefix_with_date : {target_prefix_with_date}')
        file_name = error_df.first()["file_name"]
        logger.info(f'file_name : {file_name}')

        error_df = error_df \
            .withColumn('Process',lit(process)) \
            .withColumnRenamed('batch_id','Error Log ID') \
            .withColumnRenamed('bill_id','Enhanced Bill ID') \
            .withColumnRenamed('txm_invoice_number','TXM Invoice Number') \
            .withColumnRenamed('error_message','Error') \
            .withColumnRenamed('unique_bill_id','Trading Partner Bill ID') \
            .withColumnRenamed('field_name_1','Field name 1') \
            .withColumnRenamed('field_value_1','Field Value 1') \
            .withColumnRenamed('field_name_2','Field Name 2') \
            .withColumnRenamed('field_value_2','Field Value 2') \
            .withColumnRenamed('update_timestamp','TimeStamp') \
            .select(col('Trading Partner Bill ID'),col('Enhanced Bill ID'),col('Error Log ID'),col('TXM Invoice Number'),col('Process'),col('Error'),col('Field Name 1'),col('Field Value 1'),col('Field Name 2'),col('Field Value 2'),col('TimeStamp'))

        error_df.printSchema()
        error_df.show(10, truncate=False)
        recordCount = error_df.count()
        logger.info(f"Error Mail Record Count => {recordCount}")

        error_df.coalesce(1).write.option('header','true').mode('append').format('csv').save(f"s3://{bucket_name}/{source_prefix_with_date}")
        source=source.upper()
        if trading_partner and trading_partner.strip() and trading_partner.lower() != "null":
            source_tp_str = trading_partner
            trading_partner=trading_partner.upper()
        else:
            source_tp_str = source
        source_tp_str = source_tp_str.upper()


        s3_client = boto3.client('s3','us-east-1')
        target_file_name = f"{source}_{trading_partner}_{file_header_id}_{process.replace(' ','')}_{timestamp}.csv"
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
                if trading_partner=="JOPARI":
                    trading_partner= f'{trading_partner} SOLUTIONS INC'
                    source_tp_str = f'{trading_partner} SOLUTIONS INC'
                else:
                    trading_partner=trading_partner

                # Invoke the Lambda function to send an email with the attachment
                client = boto3.client('lambda',region_name='us-east-1')
                payload = {
                    's3Bucket': bucket_name,
                    's3Key': target_file,
                    'to': error_alerts_to_email_ids,
                    'subject': f'{env_profile} - {source_tp_str} - {process}',
                    'html': f"""<p>Please review the attached CSV for the errors encountered during {process}</p>
                                <p>Source: {source}</p>
                                <p>Trading partner: {trading_partner}</p>
                                <p>Batch ID: {file_header_id}</p>
                                <p>File Name:{file_name}</p>"""
                           }

                try:
                    response = client.invoke(
                        FunctionName= lambda_function_nodemailer,  #  smtp_file_test (python lambda)
                        InvocationType='RequestResponse',
                        Payload=json.dumps(payload)
                    )

                    response_payload = json.loads(response['Payload'].read())
                    logger.info(f'Lambda response: {response_payload}')

                except Exception as e:
                    logger.error(f"Error invoking Lambda function: {e}")

        else:
            logger.info("File not available in the specified S3 path.")

    except Exception as e:
        logger.error(f"Error: {e}")
        raise e
    
def invoke_nuxeo(bill_id,txm_invoice_number,batch_id, retries, retry_delay, retry_multiplier):
    print('Invoke nuxeo lambda function')
    payload = {
        "billId":str(bill_id),
        "batchId":batch_id
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
                print(f"billId status=>{bill_id},{statusCode},{lambdaMessage}")

                if statusCode=='200':
                    if lambdaMessage:
                        storage_request_response_status = lambdaMessage
                    else:
                        storage_request_response_status='SUCCESS'
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
            v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

            # Capture error details in error log table
            # Add required columns as per error table txm_bitx_enrichment_logs.batch_storage_request_error_details (except error_id)
            error_insert_sql = f"""INSERT INTO txm_bitx_enrichment_logs.batch_storage_request_error_details (batch_id,bill_id,txm_invoice_number,source,target,storage_type,error_code,error_type,error_message,create_user,update_user,create_timestamp,update_timestamp)
                                VALUES ({batch_id},{bill_id},{txm_invoice_number},'BITX','NUXEO','STORAGE_REQUEST','Invoke Lambda','{errorInfo}','{errorMessage}','Glue - ETL - Storage Load','Glue - ETL - Storage Load','{v_load_timestamp}','{v_load_timestamp}')"""

            run_sql_query(error_insert_sql,secrets)        
            print(f'Loaded error details into RDS table txm_bitx_enrichment_logs.batch_storage_request_error_details.')
            print(f'Retry delayed {delay} for Attempt {attempt + 1}...')
            time.sleep(delay)
            delay = retry_delay * retry_multiplier
    return storage_request_response_status

def invoke_docdroid(trading_partner,invoice_header_id,bill_id,txm_invoice_number,batch_id, retries, retry_delay, retry_multiplier):
    print('Invoke docdroid lambda function')
    payload = {
        "tp":trading_partner,
        "invoiceHeaderId":invoice_header_id,
        "billId":bill_id,
        "batchId":batch_id
    }
    print(f"Payload to Lambda: {json.dumps(payload)}")
    delay = retry_delay
    bill_image_creation_response_status=None
    boto_config = Config(connect_timeout=120, read_timeout=300)
    lambda_client = boto3.client('lambda',region_name='us-east-1',config=boto_config)
    
    for attempt in range(0,retries + 1):
        print(f"Attemp: {attempt}")
        try:
            response = lambda_client.invoke(
                FunctionName=docdroid_lambda_function,
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
                print(f"billId status=>{bill_id},{statusCode},{lambdaMessage}")

                if statusCode=='200':
                    if lambdaMessage:
                        bill_image_creation_response_status = lambdaMessage
                    else:
                        bill_image_creation_response_status='SUCCESS'
                else:
                    if lambdaMessage:
                        bill_image_creation_response_status = lambdaMessage
                    else:
                        bill_image_creation_response_status='FAILED IN LAMBDA'
                        raise Exception(f"Lambda response Body not in expected format. {response_payload}")
            else:
                bill_image_creation_response_status='FAILED IN LAMBDA'
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
            bill_image_creation_response_status="FAILED IN LAMBDA"
            errorInfo=f"Error occured while invoking DocDroid lambda function (Attempt - {attempt})"
            errorMessage=str(e).replace("'","''").replace("\\","\\\\")
            print(f"errorInfo => {errorInfo}")  
            print(f"Error: {errorMessage}")
            v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

            # Capture error details in error log table
            # Add required columns as per error table txm_bitx_enrichment_logs.batch_image_creation_error_details (except error_id)
            error_insert_sql = f"""INSERT INTO txm_bitx_enrichment_logs.batch_image_creation_error_details (batch_id,bill_id,txm_invoice_number,source,target,image_type,error_code,error_type,error_message,create_user,update_user,create_timestamp,update_timestamp)
                                VALUES ({batch_id},{bill_id},{txm_invoice_number},'BITX','DOCDROID','BILL_IMAGE_CREATION','Invoke Lambda','{errorInfo}','{errorMessage}','Glue - ETL - Storage Load','Glue - ETL - Storage Load','{v_load_timestamp}','{v_load_timestamp}')"""

            run_sql_query(error_insert_sql,secrets)        
            print(f'Loaded error details into RDS table txm_bitx_enrichment_logs.batch_image_creation_error_details.')
            print(f'Retry delayed {delay} for Attempt {attempt + 1}...')
            time.sleep(delay)
            delay = retry_delay * retry_multiplier
    return bill_image_creation_response_status
     
## @params: [claimant-billingestion-storage-load-job]
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'env_profile',
    'source',
    'trading_partner',
    'file_header_id',
    'db_secret_key',
    'sns_topic_arn',
    's3_bucket',
    'docdroid_lambda_function',
    'nuxeo_lambda_function',
    'lambda_function_nodemailer',
    's3_error_attachment_prefix',
    'error_alerts_to_email_ids',
    'retries',
    'retry_delay',
    'retry_multiplier',
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
source=args['source']
trading_partner=args['trading_partner']
file_header_id=args['file_header_id']
db_secret_key=args['db_secret_key']
sns_topic_arn=args['sns_topic_arn']
s3_bucket=args['s3_bucket']
docdroid_lambda_function=args['docdroid_lambda_function']
nuxeo_lambda_function=args['nuxeo_lambda_function']
lambda_function_nodemailer = args['lambda_function_nodemailer']
s3_error_attachment_prefix=args['s3_error_attachment_prefix']
error_alerts_to_email_ids = args['error_alerts_to_email_ids']
retries=int(args['retries'])
retry_delay=int(args['retry_delay'])
retry_multiplier=int(args['retry_multiplier'])
reprocess_flag = args.get('reprocess_flag', 'N').upper()  # Default to N if not present

#Initializations
is_non_ebill = False
is_appeal_check = False
is_manual_claim_match = False
is_auto_claim_match = False
is_nuxeo_error = False
is_docdroid_error = False
is_skip_execution = False
is_skip_no_data_error = False
is_data_avaiable = False
is_no_data_execution = False
log_status = ""
status="COMPLETED"
mode="append"

try:
    logger.info(f"Processing STARTED for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id}...")

    logger.info(f"db_secret_key => {db_secret_key}")
    secrets = get_secret(db_secret_key, 'us-east-1')

    db_sp_params = source, trading_partner, file_header_id, None, job_id, 'Glue - ETL', 'Glue - ETL', 0
    logger.info(f"db_sp_params => {db_sp_params}")
    dbStoredProcedure = "txm_bitx_enrichment_logs.insert_storage_request_logs"
    logger.info(f"dbStoredProcedure => {dbStoredProcedure}")
    batch_id = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, 7)
    logger.info(f"batch_id => {batch_id}")

    file_name = ""
    if source == 'ebill':
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
            
        elif trading_partner == 'optum':
            read_db_sql = f"""
                SELECT sf.*
                FROM txm_bitx_staging.optum_file_header sf
                WHERE sf.file_header_id = {file_header_id}"""
        else:
            logger.warn(f"Trading Partner not matching => {trading_partner}")
            read_db_sql = ""
        
        if read_db_sql:
            file_header_df=get_df_db_data_with_query(read_db_sql, secrets)
            file_header_df=file_header_df.cache()
            file_header_df.printSchema()
            file_header_df.show(5, truncate=False)
            recordCount = file_header_df.count()
            logger.info(f"File Header Record Count => {recordCount}")

            if recordCount > 0:
                file_name = file_header_df.first()["file_name"]
            else:
                logger.warn(f"No data for File Header ID => {file_header_id}")
                file_name = ""
    
    logger.info(f"bill_file_name => {file_name}")

    logger.info(f"reprocess_flag => {reprocess_flag}")
    
    read_db_sql = f"""
        SELECT * FROM txm_bitx_enrichment_logs.batch_storage_request_master 
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
            if log_status in ['COMPLETED','COMPLETED SKIP REPROCESS','COMPLETED SKIP NO DATA']:
                is_skip_no_data_error = True
                logger.info(f"Existing entry found for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id} with status as {log_status} for reprocessing flow. Bypassing to further processing for CLAIM_MATCH_MANUAL FOUND flow!!!")
            else:
                logger.info(f"Existing entry found for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id} with status as {log_status} for reprocessing flow. Reprocessing flow started...")
        else:
            #Error Flow
            error_message = f"CustomError: Duplicate Batch, Log entry already exists for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id} with reprocess_flag as {reprocess_flag} (not as Y)."
            logger.error(f"error_message => {error_message}")
            raise Exception(error_message)
    else:
        logger.info(f"No existing entry found for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id}. Proceeding with storage load...")

    if not is_skip_execution:
        if source.lower() == 'ebill':
            if reprocess_flag == 'Y':
                status_where_condition_nuxeo = f""" UNION ALL
		                                            SELECT DISTINCT b.bill_id
                                                            ,b.txm_invoice_number
                                                            ,b.resubmission_code
                                                            ,COALESCE(ss2.status_type,ss.status_type) AS status_type
                                                            ,COALESCE(ss2.step_status,ss.step_status) AS status
                                                            ,b.bill_header_id AS invoice_header_id
                                                            ,COUNT(d.billing_documents_id) OVER(PARTITION BY d.bill_id) AS attachment_count
                                                            ,'N' AS send_bill_image_creation
                                                    FROM txm_bitx.bill_header b
                                                    JOIN txm_bitx.invoice_status i ON b.txm_invoice_number = i.txm_invoice_number
                                                    JOIN txm_bitx.invoice_step_status ss ON i.status_id = ss.status_id
                                                    JOIN txm_bitx.invoice_step_status tss ON i.status_id = tss.status_id
                                                    JOIN txm_bitx.billing_documents d ON b.bill_id = d.bill_id AND d.file_type = 'ATTACHMENT'
                                                    LEFT JOIN txm_bitx.invoice_step_status ss2 ON i.status_id = ss2.status_id AND ss2.status_type = 'CLAIM_MATCH_MANUAL' AND ss2.step_status = 'FOUND'
                                                    WHERE b.source='{source}'
                                                    AND b.trading_partner='{trading_partner}'
                                                    AND tss.status_type = 'STORAGE_REQUEST' AND tss.step_status = 'FAILED'
                                                    AND ss.status_type = 'CLAIM_MATCH_AUTO' AND ss.step_status = 'FOUND'
                                                    AND d.attachment_control_number IS NOT NULL AND d.document_path IS NOT NULL AND d.document_serial_number IS NULL
                                                    """
                status_where_condition_docdroid = f"""UNION ALL
                                                      SELECT DISTINCT b.bill_id
                                                            ,b.txm_invoice_number
                                                            ,b.resubmission_code
                                                            ,COALESCE(ss2.status_type,ss.status_type) AS status_type
                                                            ,COALESCE(ss2.step_status,ss.step_status) AS status
                                                            ,b.bill_header_id AS invoice_header_id
                                                            ,0 AS attachment_count
                                                            ,'Y' AS send_bill_image_creation
                                                    FROM txm_bitx.bill_header b
                                                    JOIN txm_bitx.invoice_status i ON b.txm_invoice_number = i.txm_invoice_number
                                                    JOIN txm_bitx.invoice_step_status ss ON i.status_id = ss.status_id
                                                    LEFT JOIN txm_bitx.invoice_step_status ss2 ON i.status_id = ss2.status_id AND ss2.status_type = 'CLAIM_MATCH_MANUAL' AND ss2.step_status = 'FOUND'
                                                    WHERE b.source='{source}'
                                                    AND b.trading_partner='{trading_partner}'
                                                    AND i.status_type = 'BILL_IMAGE_CREATION' AND i.status = 'FAILED'
                                                    AND ss.status_type = 'CLAIM_MATCH_AUTO' AND ss.step_status = 'FOUND'"""
                if trading_partner == 'jopari':
                    status_where_condition = status_where_condition_nuxeo
                elif trading_partner in ('align','techhealth'):
                    status_where_condition = f"""{status_where_condition_nuxeo}{status_where_condition_docdroid}"""
                else:
                    status_where_condition = status_where_condition_docdroid
            else:
                status_where_condition = ""
            
            if trading_partner.lower() == 'jopari':
                read_db_sql = f"""SELECT DISTINCT
                                    b.bill_id,
                                    b.txm_invoice_number,
                                    b.resubmission_code,
                                    COALESCE(ss.status_type, i.status_type) AS status_type, 
                                    COALESCE(ss.step_status, i.status) AS status,          
                                    b.bill_header_id,
                                    0 AS attachment_count,
                                    'N' AS send_bill_image_creation
                                FROM txm_bitx.bill_header b
                                JOIN txm_bitx.invoice_status i ON b.txm_invoice_number = i.txm_invoice_number
                                LEFT JOIN txm_bitx.invoice_step_status ss ON i.status_id = ss.status_id AND ss.status_type = 'CLAIM_MATCH_MANUAL' AND ss.step_status = 'FOUND'
                                WHERE b.source = '{source}' 
                                AND b.trading_partner = '{trading_partner}'
                                AND i.status_type = 'CLAIM_MATCH_AUTO' AND i.status = 'FOUND'
                                {status_where_condition}
                """
            else:
                read_db_sql = f"""SELECT DISTINCT b.bill_id
                                        ,b.txm_invoice_number
                                        ,b.resubmission_code
                                        ,COALESCE(ss.status_type, i.status_type) AS status_type
										,COALESCE(ss.step_status, i.status) AS status
                                        ,b.bill_header_id AS invoice_header_id
                                        ,COUNT(d.billing_documents_id) OVER(PARTITION BY d.bill_id) AS attachment_count
                                        ,'Y' AS send_bill_image_creation
                                FROM txm_bitx.bill_header b
                                JOIN txm_bitx.invoice_status i ON b.txm_invoice_number = i.txm_invoice_number
                                LEFT JOIN txm_bitx.invoice_step_status ss ON i.status_id = ss.status_id AND ss.status_type = 'CLAIM_MATCH_MANUAL' AND ss.step_status = 'FOUND'
                                LEFT JOIN txm_bitx.billing_documents d ON b.bill_id = d.bill_id AND d.file_type = 'ATTACHMENT' AND d.document_path IS NOT NULL
                                WHERE b.source='{source}'
                                AND b.trading_partner='{trading_partner}'
                                AND i.status_type = 'CLAIM_MATCH_AUTO' AND i.status = 'FOUND'
                                {status_where_condition}
                """
        else:
            if reprocess_flag == 'Y':
                status_where_condition_non_ebill = """AND (   (i.status_type = 'CLAIM_MATCH_AUTO' AND i.status = 'FOUND')
                                                           OR (i.status_type IN ('BILL_IMAGE_CREATION', 'STORAGE_REQUEST') AND i.status = 'FAILED'))"""
            else:
                status_where_condition_non_ebill = """AND i.status_type = 'CLAIM_MATCH_AUTO' AND i.status = 'FOUND'"""
            
            read_db_sql = f"""SELECT b.bill_id
                                    ,b.txm_invoice_number
                            FROM txm_bitx.bill_header b
                            JOIN txm_bitx.invoice_status i ON b.txm_invoice_number = i.txm_invoice_number
                            LEFT JOIN txm_bitx.invoice_step_status ss ON i.status_id = ss.status_id AND ss.status_type = 'CLAIM_MATCH_MANUAL' AND ss.step_status = 'FOUND'
                            WHERE b.source='{source}'
                            AND b.trading_partner IS NULL
                            {status_where_condition_non_ebill}
                        """

        bill_data_df = get_df_db_data_with_query(read_db_sql, secrets)
        bill_data_df = bill_data_df.cache()
        bill_data_df.printSchema()
        bill_data_df.show(5, truncate=False)
        bill_data_count = bill_data_df.count()
        logger.info(f"Bill data claim match Record Count => {bill_data_count}")

        is_non_ebill = False
        is_appeal_check = False
        is_manual_claim_match = False
        is_auto_claim_match = False

        if bill_data_count > 0:
            if source.lower() == 'ebill':
                is_non_ebill = False
                is_appeal_check = True
                is_manual_claim_match = False
                is_auto_claim_match = False

                bill_data_appeal_df = bill_data_df.filter(bill_data_df.resubmission_code == '30')
                bill_data_appeal_df.printSchema()
                bill_data_appeal_df.show(5, truncate=False)
                bill_data_appeal_count = bill_data_appeal_df.count()
                logger.info(f"Bill data appeal Record Count => {bill_data_appeal_count}")

                #Dataframe with resubmission_code = '30'
                if bill_data_appeal_count > 0:
                    is_data_avaiable = True
                    logger.info("Appeal flow started")

                    logger.info(f"Update invoice_status table...")
                    status_type = 'ADJUDICATION'
                    status = 'READY_FOR_SUBMISSION'
                    update_invoice_status(bill_data_appeal_df, secrets, status_type, status)
                else:
                    logger.info(f"No Data Avaialble for APPEAL FLOW!!!")

                #updated log table
                db_sp_params = batch_id, None, None, None, 'APPEAL FLOW COMPLETED', 'Glue', None, None, 'Glue - ETL', 'Glue - ETL'
                logger.info(f"db_sp_params => {db_sp_params}")
                dbStoredProcedure = "txm_bitx_enrichment_logs.update_storage_request_log_status"
                logger.info(f"dbStoredProcedure => {dbStoredProcedure}")
                res = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, None)
                logger.info(f"res => {res}")

                is_non_ebill = False
                is_appeal_check = False
                is_manual_claim_match = True
                is_auto_claim_match = False

                logger.info("New bill flow started")

                # DataFrame without resubmission_code = '30'
                new_bill_data_df = bill_data_df.filter((bill_data_df.resubmission_code != '30') | (bill_data_df.resubmission_code.isNull()) | (bill_data_df.resubmission_code == ''))
                new_bill_data_df.show(5, truncate=False)
                new_bill_data_count = new_bill_data_df.count()
                logger.info(f"New Bill data Record Count => {new_bill_data_count}")
                
                manual_claim_new_bill_df = new_bill_data_df.filter(new_bill_data_df.status_type == 'CLAIM_MATCH_MANUAL')
                manual_claim_new_bill_df.printSchema()
                manual_claim_new_bill_df.show(5, truncate=False)
                manual_claim_new_bill_count = manual_claim_new_bill_df.count()
                logger.info(f"Manual Bill data Record Count => {manual_claim_new_bill_count}")

                if manual_claim_new_bill_count > 0:
                    is_data_avaiable = True
                    logger.info(f"manual claim match storage flow")
                    if source.lower() == 'ebill' and trading_partner.lower() == 'jopari':
                        logger.info(f"Skipping Storage flow for trading partner '{trading_partner}'")

                        logger.info(f"Update invoice_status table...")
                        status_type = 'STORAGE_REQUEST'
                        status = 'NOT_APPLICABLE'
                        update_invoice_status(manual_claim_new_bill_df, secrets, status_type, status)
                    
                    else:
                        logger.warn("Warning - No Manual Claim match flow possible for other trading partners")
                        
                #updated log table
                db_sp_params = batch_id, None, None, None, 'NEW BILL MANUAL CLAIM MATCH STORAGE FLOW COMPLETED', 'Glue', None, None, 'Glue - ETL', 'Glue - ETL'
                logger.info(f"db_sp_params => {db_sp_params}")
                dbStoredProcedure = "txm_bitx_enrichment_logs.update_storage_request_log_status"
                logger.info(f"dbStoredProcedure => {dbStoredProcedure}")
                res = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, None)
                logger.info(f"res => {res}")

                is_non_ebill = False
                is_appeal_check = False
                is_manual_claim_match = False
                is_auto_claim_match = True

                auto_claim_new_bill_df = new_bill_data_df.filter(new_bill_data_df.status_type == 'CLAIM_MATCH_AUTO')
                auto_claim_new_bill_df = auto_claim_new_bill_df.cache()
                auto_claim_new_bill_df.printSchema()
                auto_claim_new_bill_df.show(5, truncate=False)
                auto_claim_new_bill_count = auto_claim_new_bill_df.count()
                logger.info(f"Auto Bill data Record Count => {auto_claim_new_bill_count}")

                if auto_claim_new_bill_count > 0:
                    is_data_avaiable = True
                    logger.info(f"Auto claim match STORAGE REQUEST flow...")
                    logger.info(f"auto_claim_new_bill_df partition count => {auto_claim_new_bill_df.rdd.getNumPartitions()}")

                    if source.lower() == 'ebill' and trading_partner.lower() in ('jopari','align','techhealth'):
                        if trading_partner == 'jopari':
                            auto_claim_new_bill_nuxeo_df = auto_claim_new_bill_df
                        elif trading_partner in ('align','techhealth'):
                            auto_claim_new_bill_nuxeo_df = auto_claim_new_bill_df.filter(auto_claim_new_bill_df.attachment_count > 0)
                            auto_claim_new_bill_nuxeo_df = auto_claim_new_bill_nuxeo_df.cache()
                            auto_claim_new_bill_nuxeo_df.printSchema()
                            auto_claim_new_bill_nuxeo_df.show(5, truncate=False)
                            auto_claim_new_bill_nuxeo_count = auto_claim_new_bill_nuxeo_df.count()
                            logger.info(f"Auto Bill data Record Count having attachment => {auto_claim_new_bill_nuxeo_count}")

                        #Register udf
                        invokeNuxeoUDF = udf(invoke_nuxeo,StringType())

                        logger.info(f"Invoke Storage Request Nuxeo lambda for trading partner '{trading_partner}'")
                        storage_request_response_df = auto_claim_new_bill_nuxeo_df.withColumn("storage_request",invokeNuxeoUDF(auto_claim_new_bill_nuxeo_df['bill_id'],auto_claim_new_bill_nuxeo_df['txm_invoice_number'],lit(batch_id),lit(retries),lit(retry_delay),lit(retry_multiplier))) 
                        storage_request_response_df = storage_request_response_df.cache()
                        storage_request_response_df.printSchema()
                        storage_request_response_df.show(5, truncate=False)
                        storage_request_response_count = storage_request_response_df.count()
                        logger.info(f"Storage Request Response Record Count => {storage_request_response_count}")

                        storage_request_response_df.groupBy('storage_request').count().show(truncate=False)

                        storage_request_lambda_error_txm_id_df=storage_request_response_df.filter(storage_request_response_df["storage_request"] == "FAILED IN LAMBDA").select("txm_invoice_number").distinct()
                        storage_request_lambda_error_txm_id_df.printSchema()
                        storage_request_lambda_error_txm_id_df.show(5, truncate=False)
                        lambdaErrorRecordCount = storage_request_lambda_error_txm_id_df.count()
                        logger.info(f"Storage Request Nuxeo Lambda Errors txm_invoice_number Record Count => {lambdaErrorRecordCount}")
                        if lambdaErrorRecordCount > 0:
                            is_nuxeo_error = True
                            logger.info(f"Found Storage Request Nuxeo Lambda Errors. Updating Invoice Status Table as FAILED!!!")
                            logger.info(f"Update invoice_status table...")
                            status_type = 'STORAGE_REQUEST'
                            status = 'FAILED'
                            update_invoice_status(storage_request_lambda_error_txm_id_df, secrets, status_type, status)

                            logger.info(f"Found Storage Request Nuxeo Lambda Errors. Overrriding Invoice Status Table as COMPLETED as NUXEO step is optional!!!")
                            logger.info(f"Update invoice_status table...")
                            status_type = 'STORAGE_REQUEST'
                            status = 'COMPLETED'
                            update_invoice_status(storage_request_lambda_error_txm_id_df, secrets, status_type, status)
                        else:
                            logger.info(f"No Storage Request Nuxeo Lambda Errors Found!!!")
                        
                        read_db_sql = f"""SELECT DISTINCT b.bill_id
                                                ,b.txm_invoice_number
                                                ,e.batch_id
                                                ,CONCAT(e.error_type, ' - ', IFNULL(REPLACE(CONVERT(e.error_message USING utf8), '"', ''''''), '')) AS error_message
                                                ,b.unique_bill_id
                                                ,'{file_name}' AS file_name
                                                ,'billing_documents.document_path' AS field_name_1
                                                ,d.document_path AS field_value_1
                                                ,'billing_documents.attachment_control_number' AS field_name_2
                                                ,d.attachment_control_number AS field_value_2
                                                ,DATE_FORMAT(CONVERT_TZ(IFNULL(e.update_timestamp, e.create_timestamp), 'UTC', 'America/Chicago'), '%Y/%m/%d %H:%i:%sCST') AS update_timestamp
                                        FROM txm_bitx.bill_header b
                                        JOIN txm_bitx_enrichment_logs.batch_storage_request_error_details e ON b.bill_id = e.bill_id
                                        LEFT JOIN txm_bitx.billing_documents d ON b.bill_id = d.bill_id AND (d.billing_documents_id = e.billing_documents_id OR e.billing_documents_id IS NULL)
                                        WHERE e.batch_id={batch_id}
                                        AND b.source = '{source}'
                                        AND b.trading_partner = '{trading_partner}'
                                        AND d.attachment_control_number IS NOT NULL AND d.document_path IS NOT NULL AND d.document_serial_number IS NULL
                            """

                        nuxeo_error_log_df = get_df_db_data_with_query(read_db_sql, secrets)
                        nuxeo_error_log_df = nuxeo_error_log_df.cache()
                        nuxeo_error_log_df.printSchema()
                        nuxeo_error_log_df.show(5, truncate=False)
                        nuxeo_error_count = nuxeo_error_log_df.count()
                        logger.info(f"Nuxeo Error Log Record Count => {nuxeo_error_count}")
                        nuxeo_error_log_txm_id_df=nuxeo_error_log_df.select("txm_invoice_number").distinct()
                        nuxeo_error_txm_id_count = nuxeo_error_log_txm_id_df.count()
                        logger.info(f"Nuxeo Error Log TxM ID Record Count => {nuxeo_error_txm_id_count}")

                        if nuxeo_error_count > 0:
                            is_nuxeo_error = True
                            logger.info(f"Found Storage Request Nuxeo Errors. Sending Consolidated Error mails with error details as attachment!!!")
                            process='Image Storage'
                            response = log_and_email_attachment(nuxeo_error_log_df, process, env_profile, file_header_id, source, trading_partner, batch_id, s3_bucket, s3_error_attachment_prefix, s3_error_attachment_prefix)
                            logger.info(f"ErrorLog File Created successfully: {response}")

                            #if auto_claim_new_bill_count == nuxeo_error_txm_id_count:
                            #    #Error Flow
                            #    error_message = f"CustomError: All Bills FAILED in the Batch. Stopping further batch processing!!!"
                            #    logger.error(f"error_message => {error_message}")
                            #    raise Exception(error_message)
                            #else:
                            #    logger.info("Found Success Bills in the Batch.")
                        else:
                            logger.info("No Storage Request Nuxeo Errors found in the Batch!!!")
                        
                    if source.lower() == 'ebill' and trading_partner.lower() != 'jopari':

                        #Register udf
                        invokeDocdroidUDF = udf(invoke_docdroid,StringType())

                        logger.info(f"Invoke DocDroid lambda for trading partner '{trading_partner}'")
                        auto_claim_new_bill_dd_df = auto_claim_new_bill_df.filter(auto_claim_new_bill_df.send_bill_image_creation == 'Y')
                        auto_claim_new_bill_dd_df = auto_claim_new_bill_dd_df.cache()
                        auto_claim_new_bill_dd_df.printSchema()
                        auto_claim_new_bill_dd_df.show(5, truncate=False)
                        auto_claim_new_bill_dd_count = auto_claim_new_bill_dd_df.count()
                        logger.info(f"Auto Bill data Record Count need to send for Bill IMage Creation => {auto_claim_new_bill_dd_count}")
                        
                        bill_image_creation_response_df = auto_claim_new_bill_dd_df.withColumn("bill_image_creation",invokeDocdroidUDF(lit(trading_partner),auto_claim_new_bill_dd_df['invoice_header_id'],auto_claim_new_bill_dd_df['bill_id'],auto_claim_new_bill_dd_df['txm_invoice_number'],lit(batch_id),lit(retries),lit(retry_delay),lit(retry_multiplier))) 
                        bill_image_creation_response_df = bill_image_creation_response_df.cache()
                        bill_image_creation_response_df.printSchema()
                        bill_image_creation_response_df.show(5, truncate=False)
                        bill_image_creation_response_count = bill_image_creation_response_df.count()
                        logger.info(f"Bill Image Creation Response Record Count => {bill_image_creation_response_count}")

                        bill_image_creation_response_df.groupBy('bill_image_creation').count().show(truncate=False)

                        bill_image_creation_lambda_error_txm_id_df=bill_image_creation_response_df.filter(bill_image_creation_response_df["bill_image_creation"] == "FAILED IN LAMBDA").select("txm_invoice_number").distinct()
                        bill_image_creation_lambda_error_txm_id_df.printSchema()
                        bill_image_creation_lambda_error_txm_id_df.show(5, truncate=False)
                        lambdaErrorRecordCount = bill_image_creation_lambda_error_txm_id_df.count()
                        logger.info(f"Bill Image Creation DocDroid Lambda Errors txm_invoice_number Record Count => {lambdaErrorRecordCount}")
                        if lambdaErrorRecordCount > 0:
                            is_docdroid_error = True
                            logger.info(f"Found Bill Image Creation DocDroid Lambda Errors. Updating Invoice Status Table as FAILED!!!")
                            logger.info(f"Update invoice_status table...")
                            status_type = 'BILL_IMAGE_CREATION'
                            status = 'FAILED'
                            update_invoice_status(bill_image_creation_lambda_error_txm_id_df, secrets, status_type, status)
                        else:
                            logger.info(f"No Bill Image Creation DocDroid Lambda Errors Found!!!")

                        read_db_sql = f"""SELECT DISTINCT b.bill_id
                                                ,b.txm_invoice_number
                                                ,e.batch_id
                                                ,CONCAT(e.error_type, ' - ', IFNULL(REPLACE(CONVERT(e.error_message USING utf8), '"', ''''''), '')) AS error_message
                                                ,b.invoice_id AS unique_bill_id
                                                ,'{file_name}' AS file_name
                                                ,'billing_documents.bill_xml_document_path' AS field_name_1
                                                ,d.bill_xml_document_path AS field_value_1
                                                ,'' AS field_name_2
                                                ,'' AS field_value_2
                                                ,DATE_FORMAT(CONVERT_TZ(IFNULL(e.update_timestamp, e.create_timestamp), 'UTC', 'America/Chicago'), '%Y/%m/%d %H:%i:%sCST') AS update_timestamp
                                                ,s.status_type
                                                ,s.status
                                        FROM txm_bitx.bill_header b
                                        JOIN txm_bitx.invoice_status s ON b.txm_invoice_number = s.txm_invoice_number
                                        JOIN txm_bitx_enrichment_logs.batch_image_creation_error_details e ON b.bill_id = e.bill_id
                                        LEFT JOIN txm_bitx.billing_documents d ON b.bill_id = d.bill_id
                                        WHERE e.batch_id={batch_id}
                                        AND b.source = '{source}'
                                        AND b.trading_partner = '{trading_partner}' 
                            """

                        docdroid_error_log_df = get_df_db_data_with_query(read_db_sql, secrets)
                        docdroid_error_log_df = docdroid_error_log_df.cache()
                        docdroid_error_log_df.printSchema()
                        docdroid_error_log_df.show(5, truncate=False)
                        docdroid_error_count = docdroid_error_log_df.count()
                        logger.info(f"Bill Image Creation DocDroid Error Log Record Count => {docdroid_error_count}")
                        docdroid_error_log_txm_id_df=docdroid_error_log_df \
                            .filter((col("status_type") == "BILL_IMAGE_CREATION") & (col("status") == "FAILED")) \
                            .select("txm_invoice_number").distinct()
                        docdroid_error_txm_id_count = docdroid_error_log_txm_id_df.count()
                        logger.info(f"Bill Image Creation DocDroid Error Log TxM ID Record Count => {docdroid_error_txm_id_count}")

                        if docdroid_error_count > 0:
                            is_docdroid_error = True
                            logger.info(f"Found Docdroid Errors. Sending Consolidated Error mails with error details with attach ")
                            process='Bill Image Creation'
                            response = log_and_email_attachment(docdroid_error_log_df, process, env_profile, file_header_id, source, trading_partner, batch_id, s3_bucket, s3_error_attachment_prefix, s3_error_attachment_prefix)
                            logger.info(f"ErrorLog File Created successfully: {response}")

                            if auto_claim_new_bill_count == docdroid_error_txm_id_count:
                                if reprocess_flag == 'Y' and log_status.startswith('COMPLETED'):
                                    logger.info("Reprocess flow - Skipping All bills failed error.")
                                else:
                                    #Error Flow
                                    error_message = f"CustomError: All Bills FAILED in the Batch. Stopping further batch processing!!!"
                                    logger.error(f"error_message => {error_message}")
                                    raise Exception(error_message)
                            else:
                                logger.info("Found Success Bills in the Batch.")
                        else:
                            logger.info("No Bill Image Creation DocDroid Error found in the Batch...")
                else:
                    logger.info(f"No Data Avaialble for NEW BILL AUTO CLAIM MATCH STORAGE FLOW!!!")

                #updated log table
                db_sp_params = batch_id, None, None, None, 'NEW BILL AUTO CLAIM MATCH STORAGE FLOW COMPLETED', 'Glue', None, None, 'Glue - ETL', 'Glue - ETL'
                logger.info(f"db_sp_params => {db_sp_params}")
                dbStoredProcedure = "txm_bitx_enrichment_logs.update_storage_request_log_status"
                logger.info(f"dbStoredProcedure => {dbStoredProcedure}")
                res = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, None)
                logger.info(f"res => {res}")
            
            else:
                is_non_ebill = True
                is_appeal_check = False
                is_manual_claim_match = False
                is_auto_claim_match = False
                if bill_data_count > 0:
                    is_data_avaiable = True
                    logger.info(f"Skip the storage request for source type => {source}")

                    logger.info(f"Update invoice_status table...")
                    status_type = 'STORAGE_REQUEST'
                    status = 'NOT_APPLICABLE'
                    update_invoice_status(bill_data_df, secrets, status_type, status)
                else:
                    logger.info(f"No Data Avaialble for NON EBILL FLOW!!!")

                #updated log table
                db_sp_params = batch_id, None, None, None, 'NON EBILL FLOW COMPLETED', 'Glue', None, None, 'Glue - ETL', 'Glue - ETL'
                logger.info(f"db_sp_params => {db_sp_params}")
                dbStoredProcedure = "txm_bitx_enrichment_logs.update_storage_request_log_status"
                logger.info(f"dbStoredProcedure => {dbStoredProcedure}")
                res = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, None)
                logger.info(f"res => {res}")
        else:
            is_no_data_execution = True
            logger.info(f"No Data Avaialble for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id}. Skipping the rest of the flow!!!")
    else:
        logger.info(f"Existing entry found for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id} with status as {log_status} for reprocessing flow. No further processing required, skipping rest of the flow!!!")
    
    #Data for log table
    if is_nuxeo_error:
        status="COMPLETED WITH NUXEO ERRORS"
    elif is_docdroid_error:
        status="COMPLETED WITH DOCDROID ERRORS"
    elif is_skip_execution:
        status="COMPLETED SKIP REPROCESS"
    elif is_skip_no_data_error:
        status="COMPLETED SKIP NO DATA"
    elif is_no_data_execution or not is_data_avaiable:
        status="FAILED NO DATA"
        #Error Flow
        error_message = f"CustomError: No Data Found in the Batch. Stopping further batch processing!!!"
        logger.error(f"error_message => {error_message}")
        raise Exception(error_message)
    else:
        status="COMPLETED"
    errorInfo="None"
    errormessage="None"
    logger.info(f"Processing COMPLETED for Source {source} trading partner {trading_partner} and File Header ID {file_header_id}!!!")
    
except Exception as e:
    if is_appeal_check:
        status="FAILED IN APPEAL CHECK FLOW"
        errorInfo="Error occured during Appeal Flow"
    elif is_non_ebill:
        status="FAILED IN NON EBILL FLOW"
        errorInfo="Error occured during Non ebill Flow"
    elif is_manual_claim_match:
        status="FAILED IN MANUAL CLAIM MATCH STORAGE FLOW"
        errorInfo="Error occured during Manual Claim Match Flow"
    elif is_auto_claim_match:
        status="FAILED IN AUTO CLAIM MATCH STORAGE FLOW"
        errorInfo="Error occured during Auto Claim Match Flow"
        if is_nuxeo_error:
            status="FAILED IN NUXEO"
            errorInfo="Error occured during Auto Claim Match Flow - NUXEO"
        elif is_docdroid_error:
            status="FAILED IN DOCDROID"
            errorInfo="Error occured during Auto Claim Match Flow - DOCDROID"
    elif "CustomError: Duplicate Batch" in str(e):
        status="FAILED DUPLICATE BATCH"
        errorInfo="Duplicate Batch"
    elif "CustomError: No Data Found in the Batch." in str(e):
        status="FAILED NO DATA"
        errorInfo="No Data found in the Batch"
    elif "CustomError: All Bills FAILED in the Batch." in str(e):
        status="FAILED ALL BILLS"
        errorInfo="All Bills FAILED in the Batch."
    else:
        status="FAILED"
        errorInfo="Error occured while batch processing"
    severity="FATAL"
    errormessage=str(e)[:500]
    logger.info("error in exception block")
    logger.info(errorInfo)  
    logger.error(str(e))
    response=log_and_email(env_profile,errorInfo,file_header_id,source,trading_partner,batch_id,errormessage,severity,sns_topic_arn)
    logger.info(f"Email sent successfully!message_id:{response['MessageId']}")
    raise e
finally:
    #updated log table
    db_sp_params = batch_id, None, None,None, status, 'Glue', errorInfo, errormessage, 'Glue - ETL', 'Glue - ETL'
    logger.info(f"db_sp_params => {db_sp_params}")
    dbStoredProcedure = "txm_bitx_enrichment_logs.update_storage_request_log_status"
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
