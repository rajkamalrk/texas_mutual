import sys
import logging
import json
import base64
import boto3
import pytz
from awsglue.job import Job
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
import pymysql
from datetime import datetime, date
from botocore.exceptions import ClientError
from botocore.client import Config
from pyspark.sql.functions import col, udf, lit
from pyspark.sql.types import StructType, StringType

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("claimant-billingestion-address-validation-job")

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
    process=f"Process : {source_tp_str} - Address Validation"
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

def invoke_address_validation(bill_id):
    payload = {"billId": bill_id}
    logger.info(f"Payload to Lambda: {json.dumps(payload)}")
    boto_config = Config(connect_timeout=120, read_timeout=300)
    lambda_client = boto3.client('lambda',region_name='us-east-1',config=boto_config)

    try:
        lambda_client.invoke(
            FunctionName=address_validation_lambda_function,
            InvocationType='RequestResponse',
            Payload=json.dumps(payload)
        )
        
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON response: {e}")
        raise e
    except Exception as e:
        logger.error(f"Error invoking Lambda function: {str(e)}")
        raise e
  

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'env_profile',
    'source',
    'trading_partner',
    'file_header_id',
    'db_secret_key',
    'sns_topic_arn',
    'address_validation_lambda_function',
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
address_validation_lambda_function=args['address_validation_lambda_function']
reprocess_flag=args.get('reprocess_flag', 'N').upper()  # Default to N if not present

#Initializations
batch_id=None
is_skip_execution = False
is_no_data_execution = False
log_status = ""
status="COMPLETED"
mode="append"
errorInfo=''
errormessage=''

try:
    logger.info(f"Processing STARTED for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id} ...")

    logger.info(f"db_secret_key => {db_secret_key}")
    secrets = get_secret(db_secret_key, 'us-east-1')

    db_sp_params = source,trading_partner, file_header_id, None, job_id, 'Glue - ETL', 'Glue - ETL', 0
    logger.info(f"db_sp_params => {db_sp_params}")
    dbStoredProcedure = "txm_bitx_enrichment_logs.insert_address_validation_logs"
    logger.info(f"dbStoredProcedure => {dbStoredProcedure}")
    batch_id = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, 7)
    logger.info(f"batch_id => {batch_id}")
    
    logger.info(f"reprocess_flag => {reprocess_flag}")

    read_db_sql = f"""
        SELECT * FROM txm_bitx_enrichment_logs.batch_address_validation_master 
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
        logger.info(f"Address Validation Flow STARTED for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id}...")

        if source == 'kofax':
            read_db_sql = f"""SELECT b.bill_id 
                            FROM txm_bitx.bill_header b LEFT JOIN 
                            txm_bitx.provider p ON b.bill_id = p.bill_id LEFT JOIN 
                            txm_bitx_provider.provider pp ON p.provider_number = pp.provider_number
                            WHERE pp.provider_number IS NULL
                            AND b.source = '{source}' 
                            AND b.file_header_id = {file_header_id}
                           """
        else:
            read_db_sql = f"""SELECT b.bill_id,b.txm_invoice_number
                            FROM txm_bitx.bill_header b
                            WHERE b.source = '{source}'
                            AND (trading_partner IS NULL OR trading_partner = '{trading_partner}')
                            AND b.file_header_id = {file_header_id}
                          """
        
        bill_data_df = get_df_db_data_with_query(read_db_sql, secrets)
        bill_data_df = bill_data_df.cache()
        bill_data_df.printSchema()
        bill_data_df.show(5, truncate=False)
        bill_data_count = bill_data_df.count()
        logger.info(f"Bill data Record Count => {bill_data_count}")

        if bill_data_count > 0:
            addressValidationUDF = udf(invoke_address_validation, StringType())
            address_validation_response_df = bill_data_df.withColumn("address_validation", addressValidationUDF(bill_data_df['bill_id']))
            address_validation_response_df = address_validation_response_df.cache()
            address_validation_response_df.printSchema()
            address_validation_response_df.show(5, truncate=False)
            address_validation_response_count = address_validation_response_df.count()
            logger.info(f"Address Validation Response Record Count => {address_validation_response_count}")

        else:
            if source.lower() == 'kofax':
                logger.info(f"Provider entry found for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id} with status as {log_status}  , No further processing required, skipping rest of the flow!!!")  
            else:
                is_no_data_execution = True
                logger.info(f"No Data Available for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id}. Skipping the rest of the flow!!!")

    else:
        logger.info(f"Existing entry found for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id} with status as {log_status} for reprocessing flow. No further processing required, skipping rest of the flow!!!")

    #Data for log table
    if is_skip_execution:
        status="COMPLETED SKIP REPROCESS"
    elif is_no_data_execution:
        status="FAILED NO DATA"
        #Error Flow
        error_message = f"CustomError: No Data Found in the Batch. Stopping further batch processing!!!"
        logger.error(f"error_message => {error_message}")
        raise Exception(error_message)
    else:
        status="COMPLETED"
    errorInfo="None"
    errormessage="None" 
    logger.info(f"Processing COMPLETED for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id}!!!")

except Exception as e:
    if "CustomError: Duplicate Batch" in str(e):
        status="FAILED DUPLICATE BATCH"
        errorInfo="Duplicate Batch"
    elif "CustomError: No Data Found in the Batch." in str(e):
        status="FAILED NO DATA"
        errorInfo="No Data found in the Batch"
    else:
        status="FAILED"
        errorInfo="Unable to process 1 or more Bill(s)"
    severity="FATAL"
    logger.error(e)
    errormessage=str(e)[:500]
    response=log_and_email(env_profile,errorInfo,file_header_id,source,trading_partner,batch_id,errormessage,severity,sns_topic_arn)
    logger.info(f"Email sent successfully!message_id:{response['MessageId']}")
    raise e
finally:
    #updated log table
    db_sp_params = batch_id, None, None,None, status, 'Glue', errorInfo, errormessage, 'Glue - ETL', 'Glue - ETL'
    logger.info(f"db_sp_params => {db_sp_params}")
    dbStoredProcedure = "txm_bitx_enrichment_logs.update_address_validation_log_status"
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