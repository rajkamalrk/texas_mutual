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
from pyspark.sql.functions import col, udf, lit
from pyspark.sql.types import StructType, StringType


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("claimant-billingestion-claim-match-load-job")

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
        ,main.update_user = 'Glue - ETL - Claim Match'
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
        SELECT s.status_id, '{status_type}', '{status}', '{status}', 'Glue - ETL - Claim Match', 'Glue - ETL - Claim Match', '{v_load_timestamp}', '{v_load_timestamp}'
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
     process=f"Process : {source_tp_str} - Claim Match (Enrichment)"
     errInfo=f"Error  : {errorInfo}"
     src=f"Source : {source}"
     fileid=f"File Header ID (TxM Staging) : {file_header_id}"

     logbatchid=f"Log Batch ID : {batch_id}"
     time=f"Timestamp : {timestamp}"
     errLog=f"Fields & Content from Error log : {errormessage}"
     log_message=f"{process}\n{errInfo}\n{src}\n{fileid}\n{logbatchid}\n{time}"
     logger.error(log_message)
     subject=f"{env_profile} - {source_tp_str} - {errorInfo} -{severity}"
     
     response=sns_client.publish(
        TopicArn=sns_topic_arn,
        Subject=subject,
        Message=f"{process}\n{errInfo}\n{src}\n{fileid}\n{logbatchid}\n{time}\n{errLog}"
        )
     return response

def log_and_email_attachment(error_df, process, env_profile, file_header_id, source, trading_partner, batch_id, bucket_name,file_name, source_prefix, target_prefix):
    logger.info("inside log email attachment method")
    try:
        current_date = datetime.now().strftime('%Y-%m-%d')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        cst_time = datetime.now(pytz.timezone('America/Chicago')).strftime('%Y/%m/%d %H:%M:%SCST')
        source_prefix_with_date = f"{source_prefix}/{current_date}/"
        target_prefix_with_date = f"{target_prefix}/{current_date}/"
        logger.info(f'source_prefix_with_date : {source_prefix_with_date}')
        logger.info(f'target_prefix_with_date : {target_prefix_with_date}')

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
                        FunctionName=lambda_function_nodemailer,  #  smtp_file_test (python lambda)
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

def invoke_claim_match(bill_id,batch_id,source,trading_partner,txm_invoice_number, retries, retry_delay, retry_multiplier):
    print('Invoke Claim Match lambda function')
    payload = {
        "billId": bill_id,
        "batchId":batch_id,
        "source":source,
        "tradingPartner":trading_partner
    }
    print(f"Payload to Lambda: {json.dumps(payload)}")
    delay = retry_delay
    claim_match_response_status=None
    boto_config = Config(connect_timeout=120, read_timeout=300)
    lambda_client = boto3.client('lambda',region_name='us-east-1',config=boto_config)
    
    for attempt in range(0,retries + 1):
        print(f"Attempt: {attempt}")
        try:
            response = lambda_client.invoke(
                FunctionName=claim_match_lambda_function,
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
                lambdaStatus = response_body_payload.get('status')
                lambdaMessage = response_body_payload.get('message')
                lambdaErrorMessage = response_body_payload.get('errorMessage')
                print(f"billId status=>{bill_id},{statusCode},{lambdaStatus},{lambdaMessage},{lambdaErrorMessage}")
                if lambdaStatus=='success':
                    claim_match_response_status='SUCCESS'
                elif lambdaStatus=='error' and 'Error occurred while invoking GW API auto claim match'.lower() in lambdaErrorMessage.lower():
                    claim_match_response_status='FAILED IN AUTO CLAIM MATCH'
                elif lambdaStatus=='error' and 'Error occurred while invoking ECI API manual claim match'.lower() in lambdaErrorMessage.lower():
                    claim_match_response_status='FAILED IN MANUAL CLAIM MATCH'
                else:
                    if lambdaErrorMessage:
                        claim_match_response_status = lambdaErrorMessage
                    else:
                        claim_match_response_status='FAILED IN LAMBDA'
                        raise Exception(f"Lambda response Body not in expected format. {response_payload}")
            else:
                claim_match_response_status='FAILED IN LAMBDA'
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
            claim_match_response_status='FAILED IN LAMBDA'
            errorInfo=f"Error occured while invoking claim match lambda function (Attempt - {attempt})"
            errorMessage=str(e).replace("'","''").replace("\\","\\\\")
            print(f"errorInfo => {errorInfo}")  
            print(f"Error: {errorMessage}")
            v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

            # Capture error details in error log table
            # Add required columns as per error table txm_bitx_enrichment_logs.batch_claim_match_error_details (except error_id)
            error_insert_sql = f"""INSERT INTO txm_bitx_enrichment_logs.batch_claim_match_error_details (batch_id,bill_id,txm_invoice_number,source,target,match_type,error_code,error_type,error_message,create_user,update_user,create_timestamp,update_timestamp)
                              VALUES ({batch_id},{bill_id},{txm_invoice_number},'BITX','CLAIM_CENTER','CLAIM_MATCH','Invoke Lambda','{errorInfo}','{errorMessage}','Glue - ETL - Claim Match Load','Glue - ETL - Claim Match Load','{v_load_timestamp}','{v_load_timestamp}')"""

            run_sql_query(error_insert_sql,secrets)        
            print(f'Loaded error details into RDS table txm_bitx_enrichment_logs.batch_claim_match_error_details.')
            print(f'Retry delayed {delay} for Attempt {attempt + 1}...')
            time.sleep(delay)
            delay = retry_delay * retry_multiplier
        
    return claim_match_response_status
        
## @params: [claimant-billingestion-claim-match-load-job-]
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'env_profile',
    'source',
    'trading_partner',
    'file_header_id',
    'db_secret_key',
    'sns_topic_arn',
    's3_bucket',
    'claim_match_lambda_function',
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
claim_match_lambda_function=args['claim_match_lambda_function']
lambda_function_nodemailer = args['lambda_function_nodemailer']
s3_error_attachment_prefix=args['s3_error_attachment_prefix']
error_alerts_to_email_ids=args['error_alerts_to_email_ids']
retries=int(args['retries'])
retry_delay=int(args['retry_delay'])
retry_multiplier=int(args['retry_multiplier'])
reprocess_flag=args.get('reprocess_flag', 'N').upper()  # Default to N if not present

#Initializations
is_claim_match = False
is_claim_data = False
is_claim_match_error = False
is_skip_execution = False
is_no_data_execution = False
log_status = ""
status="COMPLETED"
mode="append"

try:
    logger.info(f"Processing STARTED for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id}...")

    logger.info(f"db_secret_key => {db_secret_key}")
    secrets = get_secret(db_secret_key, 'us-east-1')

    db_sp_params = source,trading_partner, file_header_id, None, job_id, 'Glue - ETL', 'Glue - ETL', 0
    logger.info(f"db_sp_params => {db_sp_params}")
    dbStoredProcedure = "txm_bitx_enrichment_logs.insert_claim_match_logs"
    logger.info(f"dbStoredProcedure => {dbStoredProcedure}")
    batch_id = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, 7)
    logger.info(f"batch_id => {batch_id}")
    
    logger.info(f"reprocess_flag => {reprocess_flag}")
    
    read_db_sql = f"""
        SELECT * FROM txm_bitx_enrichment_logs.batch_claim_match_master 
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
                #is_skip_execution = True
                logger.info(f"Existing entry found for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id} with status as {log_status} for reprocessing flow. Bypassing to further processing for CLAIM_MATCH_MANUAL FOUND flow!!!")
            else:
                logger.info(f"Existing entry found for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id} with status as {log_status} for reprocessing flow. Reprocessing flow started...")
        else:
            #Error Flow
            error_message = f"CustomError: Duplicate Batch, Log entry already exists for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id} with reprocess_flag as {reprocess_flag} (not as Y)."
            logger.error(f"error_message => {error_message}")
            raise Exception(error_message)
    else:
        logger.info(f"No existing entry found for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id}. Proceeding with claim match...")

    if not is_skip_execution:
        if reprocess_flag == 'Y':
            status_where_condition = f"""AND (   (b.file_header_id = {file_header_id} AND i.status_type = 'ENRICHMENT' AND i.status = 'OPEN')
                                              OR (i.status_type IN ('CLAIM_MATCH_AUTO', 'CLAIM_MATCH_MANUAL') AND i.status = 'FAILED')
                                              OR (i.status_type IN ('CLAIM_MATCH_AUTO') AND i.status = 'IN_PROGRESS' AND i.update_timestamp < NOW() - INTERVAL 1 DAY)
                                              OR (i.status_type = 'CLAIM_MATCH_AUTO' AND i.status = 'REPROCESS') 
                                             ) """
        else:
            status_where_condition = f"""AND (   (b.file_header_id = {file_header_id} AND i.status_type = 'ENRICHMENT' AND i.status = 'OPEN')
                                              OR (i.status_type = 'CLAIM_MATCH_AUTO' AND i.status = 'REPROCESS')
                                             ) """
        
        read_db_sql = f"""
        SELECT b.bill_id
            ,b.txm_invoice_number
        FROM txm_bitx.bill_header b
        JOIN txm_bitx.invoice_status i ON b.txm_invoice_number = i.txm_invoice_number
        WHERE b.source = '{source}'
        AND (b.trading_partner IS NULL OR b.trading_partner = '{trading_partner}')
        {status_where_condition}
        """

        bill_data_df = get_df_db_data_with_query(read_db_sql, secrets)
        bill_data_df = bill_data_df.cache()
        bill_data_df.printSchema()
        bill_data_df.show(5, truncate=False)
        bill_data_count = bill_data_df.count()
        logger.info(f"Bill data Record Count => {bill_data_count}")

        logger.info(f"bill_data_df partition count => {bill_data_df.rdd.getNumPartitions()}")
        
        if bill_data_count > 0:
            if (source.lower() == 'ebill' and trading_partner.lower() == 'jopari') or source.lower() != 'ebill':
                is_claim_match = True
                is_claim_data = False

                #Register udf
                claimMatchUDF = udf(invoke_claim_match, StringType())

                logger.info('Invoking Claim Match Lambda...')
                claim_match_response_df = bill_data_df.withColumn("claim_match",claimMatchUDF(bill_data_df['bill_id'],lit(batch_id),lit(source),lit(trading_partner),bill_data_df['txm_invoice_number'],lit(retries),lit(retry_delay),lit(retry_multiplier)))
                claim_match_response_df = claim_match_response_df.cache()
                claim_match_response_df.printSchema()
                claim_match_response_df.show(5, truncate=False)
                claim_match_response_count = claim_match_response_df.count()
                logger.info(f"Claim Match Response Record Count => {claim_match_response_count}")

                claim_match_response_df.groupBy('claim_match').count().show(truncate=False)
                claim_match_lambda_error_txm_id_df = claim_match_response_df.filter(claim_match_response_df["claim_match"] == "FAILED IN LAMBDA").select("txm_invoice_number").distinct()
                claim_match_lambda_error_txm_id_df.printSchema()
                claim_match_lambda_error_txm_id_df.show(5, truncate=False)
                lambdaErrorRecordCount = claim_match_lambda_error_txm_id_df.count()
                logger.info(f"claim match Lambda Errors txm_invoice_number Record Count => {lambdaErrorRecordCount}")
                if lambdaErrorRecordCount > 0:
                    logger.info(f"Found Claim match Lambda Errors. Updating Invoice Status Table as FAILED!!!")
                    logger.info(f"Update invoice_status table...")
                    status_type = 'CLAIM_MATCH_AUTO'
                    status = 'FAILED'
                    update_invoice_status(claim_match_lambda_error_txm_id_df, secrets, status_type, status)
                else:
                    logger.info(f"No claim match Lambda Errors Found!!!")

                #Fetch file_name based on input file_header_id with respect to input trading partner (Empty in case of Kofax and ECI) 
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
                else:
                    file_name = ""
                logger.info(f"bill_file_name => {file_name}")

                read_db_sql = f"""SELECT DISTINCT b.bill_id
                                        ,b.txm_invoice_number
                                        ,e.batch_id
                                        ,CONCAT(e.error_type, ' - ', IFNULL(REPLACE(CONVERT(e.error_message USING utf8), '"', ''''''), '')) AS error_message
                                        ,CASE WHEN b.trading_partner = 'jopari' THEN b.unique_bill_id ELSE b.invoice_id END AS unique_bill_id
                                        ,'reported claim no' AS field_name_1
                                        ,b.reported_claim_number AS field_value_1
                                        ,'' AS field_name_2
                                        ,'' AS field_value_2
                                        ,DATE_FORMAT(CONVERT_TZ(IFNULL(e.update_timestamp, e.create_timestamp), 'UTC', 'America/Chicago'), '%Y/%m/%d %H:%i:%sCST') AS update_timestamp
                                        ,s.status_type
                                        ,s.status
                                FROM txm_bitx.bill_header b
                                JOIN txm_bitx.invoice_status s ON b.txm_invoice_number = s.txm_invoice_number
                                JOIN txm_bitx_enrichment_logs.batch_claim_match_error_details e ON b.bill_id = e.bill_id
                                WHERE b.source = '{source}'
                                AND (b.trading_partner IS NULL OR b.trading_partner = '{trading_partner}')
                                AND e.batch_id={batch_id}
                                """

                claim_match_error_log_df = get_df_db_data_with_query(read_db_sql, secrets)
                claim_match_error_log_df = claim_match_error_log_df.cache()
                claim_match_error_log_df.printSchema()
                claim_match_error_log_df.show(5, truncate=False)
                claim_match_error_count = claim_match_error_log_df.count()
                logger.info(f"claim match Error Log Record Count => {claim_match_error_count}")

                if claim_match_error_count > 0:
                    is_claim_match_error = True
                    logger.info(f"Found claim match Errors. Sending Consolidated Error mails with error details as attachment!!!")
                    process = 'Claim Match'
                    response = log_and_email_attachment(claim_match_error_log_df, process, env_profile, file_header_id, source, trading_partner, batch_id, s3_bucket,file_name, s3_error_attachment_prefix, s3_error_attachment_prefix)
                    logger.info(f"ErrorLog File Created successfully: {response}")

                    claim_match_error_log_txm_id_df=claim_match_error_log_df \
                        .filter(((col("status_type") == "CLAIM_MATCH_AUTO") & (col("status") == "FAILED")) | ((col("status_type") == "CLAIM_MATCH_MANUAL") & (col("status") == "FAILED"))) \
                        .select("txm_invoice_number").distinct()
                    claim_match_error_log_txm_id_df.printSchema()
                    claim_match_error_log_txm_id_df.show(5, truncate=False)
                    error_record_count = claim_match_error_log_txm_id_df.count()
                    logger.info(f"Claim Match Errors txm_invoice_number Record Count => {error_record_count}")

                    if bill_data_count == error_record_count:
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
                    logger.info("No claim match Errors found in the Batch!!!")
                
                #updated log table
                db_sp_params = batch_id, None,None,None, 'CLAIM MATCH FLOW COMPLETED', 'Glue', None, None, 'Glue - ETL', 'Glue - ETL'
                logger.info(f"db_sp_params => {db_sp_params}")
                dbStoredProcedure = "txm_bitx_enrichment_logs.update_claim_match_log_status"
                logger.info(f"dbStoredProcedure => {dbStoredProcedure}")
                res = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, None)
                logger.info(f"res => {res}")
            
            else:
                is_claim_match = False
                is_claim_data = True
                logger.info(f"Updating bill header table with claim data for trading partner: {trading_partner}")
                # Query to retrieve fields to update from claim data
                query = f"""
                SELECT b.bill_id, b.bill_header_id, b.txm_invoice_number, c.txm_claim_number, c.txm_claim_secure, c.txm_jurisdiction_code, c.txm_jurisdiction_description, c.date_of_injury
                FROM txm_bitx.bill_header b
                LEFT JOIN txm_bitx_staging.claim_data c ON c.trading_partner = b.trading_partner AND c.file_header_id = b.file_header_id AND c.bill_header_id = b.bill_header_id
                WHERE b.source = '{source}'
                AND b.trading_partner = '{trading_partner}'
                AND b.file_header_id = {file_header_id}
                """
                
                # Execute the query and get the results
                claim_data_df = get_df_db_data_with_query(query, secrets)
                claim_data_df = claim_data_df.cache()
                claim_data_df.printSchema()
                claim_data_df.show(5, truncate=False)
                claim_data_count = claim_data_df.count()
                logger.info(f"Claim Data Record Count => {claim_data_count}")
                
                # Update bill_header table fields
                update_main_table_name = f"txm_bitx.bill_header"
                logger.info(f"update_main_table_name => {update_main_table_name}")

                update_stage_table_name = f"{update_main_table_name}_{trading_partner}_{file_header_id}_stage"
                logger.info(f"update_stage_table_name => {update_stage_table_name}")
                v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

                update_db_sql = f"""
                    UPDATE txm_bitx.bill_header b 
                    JOIN {update_stage_table_name} c ON b.bill_id = c.bill_id
                    SET b.txm_claim_number = c.txm_claim_number, 
                    b.txm_claim_secure = c.txm_claim_secure, 
                    b.txm_jurisdiction_code = c.txm_jurisdiction_code,
                    b.txm_jurisdiction_description = c.txm_jurisdiction_description,
                    b.update_user = 'Glue - ETL - Claim Match',
                    b.update_timestamp = '{v_load_timestamp}'
                """

                update_db_records(claim_data_df, secrets, update_main_table_name, update_stage_table_name, update_db_sql)
                logger.info(f'Updated claim_data table fields into table txm_bitx.bill_header')

                # Update patient table fields
                update_main_table_name = f"txm_bitx.patient"
                logger.info(f"update_main_table_name => {update_main_table_name}")

                update_stage_table_name = f"{update_main_table_name}_{trading_partner}_{file_header_id}_stage"
                logger.info(f"update_stage_table_name => {update_stage_table_name}")
                v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

                update_db_sql = f"""
                    UPDATE txm_bitx.patient p 
                    JOIN {update_stage_table_name} c ON p.bill_id = c.bill_id
                    SET p.date_of_injury = c.date_of_injury,
                    p.update_user = 'Glue - ETL - Claim Match',
                    p.update_timestamp = '{v_load_timestamp}'
                """

                update_db_records(claim_data_df, secrets, update_main_table_name, update_stage_table_name, update_db_sql)
                logger.info(f'Updated claim_data table fields into table txm_bitx.patient.')
                
                # Update invoice status table 
                status_type = 'CLAIM_MATCH_AUTO'
                status = 'FOUND'
                update_invoice_status(claim_data_df, secrets, status_type, status)

                # Updated log table
                db_sp_params = batch_id, None, None, None, 'CLAIM DATA FLOW COMPLETED', 'Glue', None, None, 'Glue - ETL', 'Glue - ETL'
                logger.info(f"db_sp_params => {db_sp_params}")
                dbStoredProcedure = "txm_bitx_enrichment_logs.update_claim_match_log_status"
                logger.info(f"dbStoredProcedure => {dbStoredProcedure}")
                res = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, None)
                logger.info(f"res => {res}")
        else:
            is_no_data_execution = True
            logger.info(f"No Data Avaialble for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id}. Skipping the rest of the flow!!!")
    else:
        logger.info(f"Existing entry found for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id} with status as {log_status} for reprocessing flow. No further processing required, skipping rest of the flow!!!")

    #Data for log table
    if is_claim_match_error:
        status="COMPLETED WITH CLAIM MATCH ERRORS"
    elif is_skip_execution:
        status="COMPLETED SKIP REPROCESS"
    #elif is_no_data_execution:
        #status="FAILED NO DATA"
        #Error Flow
        #error_message = f"CustomError: No Data Found in the Batch. Stopping further batch processing!!!"
        #logger.error(f"error_message => {error_message}")
        #raise Exception(error_message)
    else:
        status="COMPLETED"
    errorInfo="None"
    errormessage="None"
        
    logger.info(f"Processing COMPLETED for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id}!!!")
    
except Exception as e:
    if is_claim_match:
        status="FAILED IN CLAIM MATCH"
        errorInfo="Error occured during Claim Match Flow"
    elif is_claim_data:
        status="FAILED IN CLAIM DATA"
        errorInfo="Error occured during Claim DATA Flow"
    elif "CustomError: Duplicate Batch" in str(e):
        status="FAILED DUPLICATE BATCH"
        errorInfo="Duplicate Batch"
    #elif "CustomError: No Data Found in the Batch." in str(e):
        #status="FAILED NO DATA"
        #errorInfo="No Data found in the Batch"
    elif "CustomError: All Bills FAILED in the Batch." in str(e):
        status="FAILED ALL BILLS"
        errorInfo="All Bills FAILED in the Batch."
    else:
        status="FAILED"
        errorInfo="Error occured while batch processing"
    severity="FATAL"
    errormessage=str(e)[:500]
    logger.info(errorInfo)  
    logger.error(str(e))
    response=log_and_email(env_profile,errorInfo,file_header_id,source,trading_partner,None,errormessage,severity,sns_topic_arn)
    logger.info(f"Email sent successfully!message_id:{response['MessageId']}")
    raise e
finally:
    #updated log table
    db_sp_params = batch_id, None, None,None, status, 'Glue', errorInfo, errormessage, 'Glue - ETL', 'Glue - ETL'
    logger.info(f"db_sp_params => {db_sp_params}")
    dbStoredProcedure = "txm_bitx_enrichment_logs.update_claim_match_log_status"
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