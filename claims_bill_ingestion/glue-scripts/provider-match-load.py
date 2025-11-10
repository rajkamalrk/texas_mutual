import sys
import logging
import json
import base64
import boto3
import pytz
from botocore.exceptions import ClientError
from awsglue.job import Job
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
import pymysql
from datetime import datetime, date
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, substring, lit, explode, split, regexp_replace
from pyspark.sql.types import StructType, StringType


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("claimant-billingestion-provider-match-job")

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
                        .option("sessionInitStatement", "SET SESSION group_concat_max_len = 1000000;") \
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

    update_stage_table_name = f"{update_main_table_name}_{source}_{trading_partner}_{file_header_id}_stage"
    logger.info(f"update_stage_table_name => {update_stage_table_name}")

    update_query = f"""
        UPDATE {update_main_table_name} main
        JOIN {update_stage_table_name} stage ON main.txm_invoice_number = stage.txm_invoice_number
        SET main.status_type = '{status_type}'
        ,main.status = '{status}'
        ,update_user = 'Glue - ETL - Provider Match'
        ,update_timestamp = '{v_load_timestamp}'
    """

    logger.info(f"Calling update_db_records function to update {update_main_table_name} in database...")
    update_db_records(input_df, secrets, update_main_table_name, update_stage_table_name, update_query)
    logger.info(f"Invoice Status table update COMPLETED for status_type {status_type} and status {status}")
    
    logger.info(f"Invoice Step Status table load STARTED for status_type {status_type} and status {status}")
    update_main_table_name = f"txm_bitx.invoice_step_status"
    logger.info(f"update_main_table_name => {update_main_table_name}")

    update_stage_table_name = f"{update_main_table_name}_{source}_{trading_partner}_{file_header_id}_stage"
    logger.info(f"update_stage_table_name => {update_stage_table_name}")

    insert_query = f"""
        INSERT INTO {update_main_table_name} (status_id, status_type, step_info, step_status, create_user, update_user, create_timestamp, update_timestamp)
        SELECT s.status_id, '{status_type}', '{status}', '{status}', 'Glue - ETL - Provider Match', 'Glue - ETL - Provider Match', '{v_load_timestamp}', '{v_load_timestamp}'
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
    

def get_provider_number(batch_size):
    provider_number_list = []
    client = boto3.client('lambda',region_name='us-east-1')
    payload = {"batchSize": batch_size}
    logger.info(f"Payload to Lambda: {json.dumps(payload)}")

    try:
        response = client.invoke(
            FunctionName=pr_num_generate_lambda_function,
            InvocationType='RequestResponse',
            Payload=json.dumps(payload)
        )
        response_payload = response['Payload'].read().decode('utf-8')
        logger.info(f"Lambda response payload: {response_payload}")
        response_payload = json.loads(response_payload)
        logger.info(f"response_payload => {response_payload}")
        
        #Check if the response is not None and contains the required data
        if response_payload and 'body' in response_payload:
            response_body_payload = response_payload['body']
            if isinstance(response_body_payload, str):
                response_body_payload = json.loads(response_body_payload)
            if 'status' in response_body_payload and response_body_payload['status'] == 'success' and 'data' in response_body_payload:
                if batch_size == 1:
                    provider_number = response_body_payload['data'].get('providerNumber')
                    if provider_number:
                        provider_number_list = [provider_number]
                    else:
                        provider_number_list = []
                        logger.error("Failed to retrieve single Provider number from Lambda response.")
                        raise Exception(f"Failed to retrieve single Provider number from Lambda response. {response_payload}")
                else:
                    # Handle multiple providerNumbers case
                    provider_number_list = response_body_payload['data'].get('providerNumbers')
                    if not provider_number_list:
                        provider_number_list = []
                        logger.error("Failed to retrieve Provider numbers from Lambda response.")
                        raise Exception(f"Failed to retrieve Provider numbers from Lambda response. {response_payload}")
            else:
                logger.error("Lambda response Body not in expected format.")
                raise Exception(f"Lambda response Body not in expected format. {response_payload}")
        else:
            logger.error("Lambda response not in expected format.")
            raise Exception(f"Lambda response not in expected format. {response_payload}")
    
        return provider_number_list
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON response: {e}")
        raise e
    except Exception as e:
        logger.error(f"Error invoking Lambda function: {str(e)}")
        raise e
    
# log and email 
def log_and_email(env_profile,errorInfo,file_header_id,source,trading_partner,errormessage,severity,sns_topic_arn):
    """logging error and sending error email"""
    logger.info("inside log email method")
    sns_client=boto3.client('sns',region_name='us-east-1')
    cst=pytz.timezone('America/Chicago')
    timestamp=datetime.now(cst).strftime('%Y/%m/%d %H:%M:%S %Z%z')
    #create log message
    if trading_partner and trading_partner.strip() and trading_partner.lower() != "null":
        source_tp_str = trading_partner
    else:
       source_tp_str = source
    source_tp_str = source_tp_str.upper()
    process=f"Process : {source_tp_str} SOLUTIONS INC - Invalid Parameters"
    errInfo=f"Error  : {errorInfo}"
    fileid=f"File Header ID (TxM Staging) : {file_header_id}"
    logId=f"Log ID : {batch_id}"
    time=f"Timestamp : {timestamp}"
    errLog=f"Fields & Content from Error log : {errormessage}"
    log_message=f"{process}\n{errInfo}\n{fileid}\n{logId}\n{time}"
    logger.error(log_message)
    subject=f"{env_profile} - {source_tp_str}  SOLUTIONS INC - {errorInfo} -{severity}"
     
    response=sns_client.publish(
        TopicArn=sns_topic_arn,
        Subject=subject,
        Message=f"{process}\n{errInfo}\n{fileid}\n{logId}\n{time}\n{errLog}"
        )
    return response

## @params: [claimant-billingestion-provider-match-job]
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'env_profile',
    'source',
    'trading_partner',
    'file_header_id',
    'db_secret_key',
    'sns_topic_arn',
    'pr_num_generate_lambda_function',
    'batch_size',
    'reprocess_flag',
    'datacatalog_schema_name',
    'datacatalog_config_table_name',
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
trading_partner = args.get('trading_partner', None)
file_header_id=args['file_header_id']
db_secret_key=args['db_secret_key']
sns_topic_arn=args['sns_topic_arn']
pr_num_generate_lambda_function=args['pr_num_generate_lambda_function']
batch_size = int(args['batch_size'])
reprocess_flag=args.get('reprocess_flag', 'N').upper()  # Default to N if not present
datacatalog_schema_name=args['datacatalog_schema_name']
datacatalog_config_table_name=args['datacatalog_config_table_name']

#Initializations
is_provider_search = False
is_provider_create = False
is_skip_execution = False
is_skip_no_data_error = False
is_no_data_execution = False
log_status = ""
mode = "append"

try:
    logger.info(f"Processing STARTED for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id}...")

    logger.info(f"db_secret_key => {db_secret_key}")
    secrets = get_secret(db_secret_key, 'us-east-1')

    db_sp_params = source, trading_partner, file_header_id, job_id, 'Glue - ETL', 'Glue - ETL', 0
    logger.info(f"db_sp_params => {db_sp_params}")
    dbStoredProcedure = "txm_bitx_enrichment_logs.insert_provider_request_logs"
    logger.info(f"dbStoredProcedure => {dbStoredProcedure}")
    batch_id = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, 6)
    logger.info(f"batch_id => {batch_id}")
    
    logger.info(f"reprocess_flag => {reprocess_flag}")
    
    read_db_sql = f"""
        SELECT * FROM txm_bitx_enrichment_logs.batch_provider_request_master 
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
        logger.info(f"No existing entry found for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id}. Proceeding with provider match...")
    
    if not is_skip_execution:
        if reprocess_flag == 'Y':
            status_where_condition = """AND (   (s.status_type IN ('VENDOR_MATCH_MANUAL', 'VENDOR_MATCH_AUTO') AND s.status IN ('FOUND','NOT_APPLICABLE'))
                                             OR (s.status_type = 'PROVIDER_MATCH_AUTO' AND s.status = 'FAILED')
                                            )"""
        else:
            status_where_condition = """AND s.status_type IN ('VENDOR_MATCH_MANUAL', 'VENDOR_MATCH_AUTO') AND s.status IN ('FOUND','NOT_APPLICABLE')"""
    
        if source.lower() == 'ebill':
            read_db_sql = f"""
            SELECT b.bill_id, b.txm_invoice_number
            FROM txm_bitx.bill_header b
            JOIN txm_bitx.invoice_status s ON b.txm_invoice_number = s.txm_invoice_number
            WHERE b.source='{source}'
            AND b.trading_partner='{trading_partner}'
            {status_where_condition}
            """

        else:
            read_db_sql = f"""
            SELECT b.bill_id, b.txm_invoice_number
            FROM txm_bitx.bill_header b
            JOIN txm_bitx.invoice_status s ON b.txm_invoice_number = s.txm_invoice_number
            WHERE b.source='{source}'
            AND b.trading_partner IS NULL
            {status_where_condition}
            """

        bill_data_df = get_df_db_data_with_query(read_db_sql, secrets)
        bill_data_df = bill_data_df.cache()
        bill_data_df.printSchema()
        bill_data_df.show(5, truncate=False)
        bill_data_count = bill_data_df.count()
        logger.info(f"Bill data vendor match Record Count => {bill_data_count}")
        
        if bill_data_count > 0:
            if source.lower() == 'ebill' or source.lower() == 'kofax':
                logger.info(f"Update invoice_status table...")
                status_type = 'PROVIDER_MATCH_AUTO'
                status = 'INPROGRESS'
                update_invoice_status(bill_data_df, secrets, status_type, status)

                logger.info("Provider Search flow started")
                is_provider_search = True
                is_provider_create = False

                if source.lower() == 'ebill':
                    read_db_sql = f"""
                    WITH provider_search_results AS (
                                                    SELECT b.bill_id, 
                                                        b.txm_invoice_number, 
                                                        bp.provider_id,
                                                        1 AS search_priority,
                                                        p.provider_number,
                                                        p.create_timestamp,
                                                        p.update_timestamp
                                                    FROM txm_bitx.bill_header b
                                                    JOIN txm_bitx.invoice_status s ON b.txm_invoice_number = s.txm_invoice_number
                                                    JOIN txm_bitx.provider bp ON b.bill_id = bp.bill_id
                                                    JOIN txm_bitx_provider.provider p
                                                    ON bp.vendor_tax_id = p.vendor_tax_id
                                                    AND IFNULL(UPPER(bp.rendering_provider_license), 'NO NUMBER') = IFNULL(UPPER(p.rendering_provider_license), 'NO NUMBER')
                                                    AND bp.rendering_provider_npi = p.rendering_provider_npi
                                                    AND IFNULL(UPPER(bp.vendor_number), 'DUMMY_VAL') = IFNULL(UPPER(p.vendor_number), 'DUMMY_VAL')
                                                    AND UPPER(bp.facility_address_1) = UPPER(p.facility_address_1)
                                                    AND UPPER(bp.facility_city) = UPPER(p.facility_city)
                                                    AND UPPER(bp.facility_state) = UPPER(p.facility_state)
                                                    AND UPPER(bp.facility_zip_code) = UPPER(p.facility_zip_code)
                                                    WHERE b.source='{source}'
                                                    AND b.trading_partner = '{trading_partner}'
                                                    AND s.status_type IN ('PROVIDER_MATCH_AUTO')
                                                    AND s.status = 'INPROGRESS'
                                                    )
                    SELECT tp.bill_id, 
                        tp.txm_invoice_number, 
                        tp.provider_id, 
                        FIRST_VALUE(tp.provider_number) OVER (PARTITION BY tp.bill_id, tp.txm_invoice_number, tp.provider_id ORDER BY tp.search_priority ASC, tp.update_timestamp ASC, tp.create_timestamp ASC) AS provider_number
                    FROM   provider_search_results tp
                    GROUP BY tp.bill_id, tp.txm_invoice_number, tp.provider_id
                    """
                else:
                    read_db_sql = f"""SELECT b.bill_id,
                        b.txm_invoice_number,
                        bp.provider_id,
                        p.provider_number
                        FROM txm_bitx.bill_header b
                        JOIN txm_bitx.invoice_status s ON b.txm_invoice_number = s.txm_invoice_number
                        JOIN txm_bitx.provider bp ON b.bill_id = bp.bill_id
                        JOIN txm_bitx_provider.provider p ON bp.provider_number = p.provider_number
                        WHERE b.source='{source}'
                        AND b.trading_partner is NULL
                        AND bp.provider_number IS NOT NULL
                        AND s.status_type IN ('PROVIDER_MATCH_AUTO')
                        AND s.status = 'INPROGRESS'"""

                bill_provider_match_df = get_df_db_data_with_query(read_db_sql, secrets)
                bill_provider_match_df = bill_provider_match_df.cache()
                bill_provider_match_df.printSchema()
                bill_provider_match_df.show(5, truncate=False)
                bill_provider_match_count = bill_provider_match_df.count()
                logger.info(f"Bill provider match Record Count => {bill_provider_match_count}")

                if bill_provider_match_count > 0:
                    logger.info(f"Found Provider match records...")
                    update_main_table_name = f"txm_bitx.provider"
                    logger.info(f"update_main_table_name => {update_main_table_name}")

                    update_stage_table_name = f"{update_main_table_name}_{source}_{trading_partner}_{file_header_id}_stage"
                    logger.info(f"update_stage_table_name => {update_stage_table_name}")
                    v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

                    query = f"""
                        UPDATE {update_main_table_name} main
                        JOIN {update_stage_table_name} stage ON main.provider_id = stage.provider_id
                        SET main.provider_number = stage.provider_number,
                        main.update_user = 'Glue - ETL - Provider Match',
                        main.update_timestamp = '{v_load_timestamp}'
                    """

                    logger.info(f"Calling update_db_records function to update {update_main_table_name} in database...")
                    update_db_records(bill_provider_match_df, secrets, update_main_table_name, update_stage_table_name, query)

                    logger.info(f"Update invoice_status table...")
                    status_type = 'PROVIDER_MATCH_AUTO'
                    status = 'FOUND'
                    bill_provider_match_txm_id_df=bill_provider_match_df.select("txm_invoice_number").distinct()
                    bill_provider_match_txm_id_df.printSchema()
                    bill_provider_match_txm_id_df.show(5, truncate=False)
                    bill_provider_match_txm_id_count = bill_provider_match_txm_id_df.count()
                    logger.info(f"Bill provider match txm_invoice_number Record Count => {bill_provider_match_txm_id_count}")
                    update_invoice_status(bill_provider_match_txm_id_df, secrets, status_type, status)

                    #updated log table
                    db_sp_params = batch_id, None, None, None,'PROVIDER SEARCH COMPLETED', 'Glue', None, None, 'Glue - ETL', 'Glue - ETL'
                    logger.info(f"db_sp_params => {db_sp_params}")
                    dbStoredProcedure = "txm_bitx_enrichment_logs.update_provider_request_log_status"
                    logger.info(f"dbStoredProcedure => {dbStoredProcedure}")
                    res = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, None)
                    logger.info(f"res => {res}")
                
                else:
                    logger.info(f"Not Found Provider match records...")

                logger.info("Provider Create flow started")
                is_provider_search = False
                is_provider_create = True
                
                # Transformation sql
                v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

                read_db_sql = f"""
                SELECT  GROUP_CONCAT(DISTINCT b.txm_invoice_number SEPARATOR ',') AS txm_invoice_number_str
                    ,GROUP_CONCAT(DISTINCT p.provider_id SEPARATOR ',') AS provider_id_str
                    ,'2' AS provider_entity_type
                    ,CASE WHEN lower(b.trading_partner) = 'optum' THEN 'TX' 
                            WHEN lower(b.trading_partner) IN ('jopari','align','techhealth') AND (b.form_type = 'PROF' OR b.form_type = 'INST') THEN RIGHT(TRIM(p.rendering_provider_license),2) 
                            ELSE NULL
                        END AS rendering_provider_jurisdiction
                    ,b.form_type
                    ,p.vendor_tax_id
                    ,'T' AS vendor_tax_id_indicator
                    ,p.vendor_number
                    ,p.vendor_name
                    ,CASE WHEN b.form_type IN ('RX','P') THEN 'RX' END AS vendor_license_type
                    ,p.vendor_address_1 AS vendor_address1
                    ,p.vendor_address_2 AS vendor_address2
                    ,p.vendor_city
                    ,p.vendor_state
                    ,p.vendor_zip_code
                    ,p.vendor_phone_number AS vendor_phone
                    ,p.facility_name
                    ,p.facility_npi
                    ,p.facility_address_1
                    ,p.facility_address_2
                    ,p.facility_city
                    ,p.facility_state
                    ,p.facility_zip_code
                    ,p.rendering_provider_npi
                    ,p.rendering_provider_name
                    ,CASE WHEN lower(b.trading_partner) = 'jopari' AND b.form_type = 'INST' AND (p.rendering_provider_license IS NULL OR TRIM(p.rendering_provider_license) = '') THEN 'NO NUMBER' ELSE p.rendering_provider_license 
                    END AS rendering_provider_license                
                    ,IFNULL(federal_military_treatment_facility,'N') AS udf_data_value
                    ,'Glue - ETL - Provider Match' AS create_user
                    ,'Glue - ETL - Provider Match' AS update_user
                    ,'{v_load_timestamp}' AS create_timestamp
                    ,'{v_load_timestamp}' AS update_timestamp     
                FROM txm_bitx.bill_header b
                JOIN txm_bitx.invoice_status s ON b.txm_invoice_number = s.txm_invoice_number
                JOIN txm_bitx.provider p ON b.bill_id = p.bill_id
                WHERE b.source='{source}'
                AND (b.trading_partner IS NULL OR b.trading_partner='{trading_partner}')
                AND s.status_type in ('PROVIDER_MATCH_AUTO')
                AND s.status = 'INPROGRESS'
                GROUP BY p.vendor_tax_id
                        ,CASE WHEN lower(b.trading_partner) = 'jopari' AND b.form_type = 'INST' AND (p. rendering_provider_license IS NULL OR TRIM(p.rendering_provider_license) = '') THEN 'NO NUMBER' ELSE IFNULL(UPPER(p.rendering_provider_license), 'DUMMY_VAL') END
                        ,p.rendering_provider_npi
                        ,IFNULL(UPPER(p.vendor_number), 'DUMMY_VAL')
                        ,UPPER(p.facility_address_1)
                        ,UPPER(p.facility_city)
                        ,UPPER(p.facility_state)
                        ,UPPER(p.facility_zip_code)
                """

                bill_provider_create_df = get_df_db_data_with_query(read_db_sql, secrets)
                bill_provider_create_df = bill_provider_create_df.cache()
                bill_provider_create_df.printSchema()
                bill_provider_create_df.show(5, truncate=False)
                bill_provider_create_count = bill_provider_create_df.count()
                logger.info(f"Bill provider create Record Count => {bill_provider_create_count}")

                if bill_provider_create_count > 0:
                    logger.info(f"Found Provider create records...")

                    logger.info(f"bill_provider_create_df partition count => {bill_provider_create_df.rdd.getNumPartitions()}")
                    
                    # Step 1: Create a DataFrame to fetch distinct `rendering_provider_license` and `form_type`
                    # Step 1: Add prefix columns to the distinct_license_form_df
                    distinct_license_form_df = bill_provider_create_df \
                        .withColumn("prefix_3", substring(col("rendering_provider_license"), 1, 3)) \
                        .withColumn("prefix_2", substring(col("rendering_provider_license"), 1, 2)) \
                        .select(col("provider_id_str"), col("rendering_provider_license"), col("form_type"), col("prefix_3"), col("prefix_2")).cache()
                    
                    logger.info("Distinct Rendering Provider License and Form Type:")
                    distinct_license_form_df.printSchema()
                    distinct_license_form_df.show(5, truncate=False)
                    recordCount = distinct_license_form_df.count()
                    logger.info(f"Rendering Provider License and Form Type Record Count => {recordCount}")
                    
                    logger.info(f'Create Temp VW based on df distinct_license_form_df...')
                    distinct_license_form_df.createOrReplaceTempView("distinct_license_form_vw")

                    provider_license_config_df = spark.table(f"{datacatalog_schema_name}.{datacatalog_config_table_name}")
                    logger.info("Static Table Data Catalog Records:")
                    provider_license_config_df.printSchema()
                    provider_license_config_df.show(5, truncate=False)
                    recordCount = provider_license_config_df.count()
                    logger.info(f"License Type Config Record Count => {recordCount}")
                    
                    provider_type_code_df = spark.sql(f"""SELECT DISTINCT p.provider_id_str
                                                            ,COALESCE(CASE WHEN p.form_type IN ('INST','H') THEN 'HP'
                                                                        WHEN p.form_type IN ('RX','P') THEN 'RX'
                                                                        WHEN p.form_type IN ('ADA','T') THEN 'DS'
                                                                      END
                                                                    ,cp_p3.provider_type_code
                                                                    ,cp_p2.provider_type_code
                                                                    ,'DR'
                                                                    ) AS provider_type_code
                                                        FROM distinct_license_form_vw p
                                                        LEFT JOIN {datacatalog_schema_name}.{datacatalog_config_table_name} cp_p3 ON p.prefix_3 = cp_p3.provider_license_prefix
                                                        LEFT JOIN {datacatalog_schema_name}.{datacatalog_config_table_name} cp_p2 ON p.prefix_2 = cp_p2.provider_license_prefix""")
                    
                    logger.info("Provider Type Code Records:")
                    provider_type_code_df.printSchema()
                    provider_type_code_df.show(5, truncate=False)
                    recordCount = provider_type_code_df.count()
                    logger.info(f"Provider Type Code Record Count => {recordCount}")
                    
                    updated_bill_provider_create_df = bill_provider_create_df.join(provider_type_code_df, 'provider_id_str', 'left').drop('form_type')
                    updated_bill_provider_create_df = updated_bill_provider_create_df.cache()
                    logger.info("Provider Create DF with Provider Type Code Records:")
                    updated_bill_provider_create_df.printSchema()
                    updated_bill_provider_create_df.show(5, truncate=False)
                    recordCount = updated_bill_provider_create_df.count()
                    logger.info(f"Updated Provider Create DF Record Count => {recordCount}")

                    # Batch processing
                    start_row = 1
                    end_row = None
                    is_create_batch = False
                    window_spec = Window.orderBy("txm_invoice_number_str")
                    batch_bill_provider_create_df = updated_bill_provider_create_df.withColumn("row_number", row_number().over(window_spec))
                    batch_bill_provider_create_df = batch_bill_provider_create_df.cache()
                    batch_bill_provider_create_df.printSchema()
                    batch_bill_provider_create_df.show(5, truncate=False)
                    recordCount = batch_bill_provider_create_df.count()
                    logger.info(f"Bill provider create with Row Number Record Count => {recordCount}")

                    # Process each batch
                    logger.info(f"Bill provider create batch based on batch_size {batch_size}...")
                    for start_row in range(1, bill_provider_create_count + 1, batch_size):
                        end_row = min(start_row + batch_size - 1, bill_provider_create_count)
                        logger.info(f"Processing batch with start row {start_row} and end row {end_row}...")
                        if start_row == 1:
                            is_create_batch = True
                        temp_batch_bill_provider_create_df = batch_bill_provider_create_df.filter((col("row_number") >= start_row) & (col("row_number") <= end_row)).cache()
                        temp_batch_bill_provider_create_df.printSchema()
                        temp_batch_bill_provider_create_df.show(5, truncate=False)
                        batch_record_count = temp_batch_bill_provider_create_df.count()
                        logger.info(f"Batch DF Record Count => {batch_record_count}")

                        # Check if data available for Batch
                        if batch_record_count > 0:
                            logger.info(f"Data Avaiable for start row {start_row} and end row {end_row}...")
                        else:
                            #Error Flow
                            error_message = f"CustomError: Data NOT Avaiable start row {start_row} and end row {end_row}."
                            logger.error(f"error_message => {error_message}")
                            raise Exception(error_message)

                        provider_number_list = get_provider_number(batch_record_count)
                        if provider_number_list:
                            # Extract the invoice number from the parsed response
                            provider_numbers = [{"provider_number": item, "row_number": start_row + idx} for idx, item in enumerate(provider_number_list)]
                            logger.info(f"provider_numbers :{provider_numbers}")

                            temp_provider_number_df = spark.createDataFrame(provider_numbers)
                            temp_provider_number_df.printSchema()
                            temp_provider_number_df.show(5, truncate=False)
                            recordCount = temp_provider_number_df.count()
                            logger.info(f"Invoice Number Generated Record Count => {recordCount}")
                        else:
                            #Error Flow
                            error_message = f"CustomError: Provider Numbers NOT Avaiable start row {start_row} and end row {end_row}."
                            logger.error(f"error_message => {error_message}")
                            raise Exception(error_message)

                        temp_updated_bill_provider_create_df = temp_batch_bill_provider_create_df.join(temp_provider_number_df,"row_number")
                        temp_updated_bill_provider_create_df = temp_updated_bill_provider_create_df.cache()
                        temp_updated_bill_provider_create_df.printSchema()
                        temp_updated_bill_provider_create_df.show(5, truncate=False)
                        record_count = temp_updated_bill_provider_create_df.count()
                        logger.info(f"Bill provider create (with Provider Number) Record Count => {record_count}")
                        
                        error_updated_bill_provider_create_df = temp_updated_bill_provider_create_df.filter(col("provider_number").isNull())
                        error_updated_bill_provider_create_df.printSchema()
                        error_updated_bill_provider_create_df.show(5, truncate=False)
                        record_count = error_updated_bill_provider_create_df.count()
                        logger.info(f"Bill provider ERROR create (with Provider Number) Record Count => {record_count}")

                        final_batch_provider_create_df = temp_updated_bill_provider_create_df.drop(*('txm_invoice_number_str', 'provider_id_str', 'row_number'))
                        print("print schema after dropping col")
                        final_batch_provider_create_df.printSchema()
                        final_batch_provider_create_df.show(5, truncate=False)
                        record_count = final_batch_provider_create_df.count()
                        logger.info(f"Bill provider create Final Record Count => {record_count}")
                        
                        provider_db_table_name = "txm_bitx_provider.provider"
                        logger.info(f'Load final_batch_provider_create_df dataframe to RDS table => {provider_db_table_name}')
                        load_data_to_rds(final_batch_provider_create_df, provider_db_table_name, secrets, mode)

                        #Update Provider Number in Enhanced DB
                        update_main_table_name = f"txm_bitx.provider"
                        logger.info(f"update_main_table_name => {update_main_table_name}")

                        update_stage_table_name = f"{update_main_table_name}_{source}_{trading_partner}_{file_header_id}_stage"
                        logger.info(f"update_stage_table_name => {update_stage_table_name}")
                        v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

                        query = f"""
                            UPDATE {update_main_table_name} main
                            JOIN {update_stage_table_name} stage ON main.provider_id = stage.provider_id
                            SET main.provider_number = stage.provider_number,
                            main.update_user = 'Glue - ETL - Provider Match',
                            main.update_timestamp = '{v_load_timestamp}'
                        """

                        bill_provider_create_provider_id_df=temp_updated_bill_provider_create_df \
                            .withColumn("provider_id", explode(split(regexp_replace(col("provider_id_str"), "(^\[)|(\]$)", ""), ","))) \
                            .select("provider_id","provider_number")
                        bill_provider_create_provider_id_df.printSchema()
                        bill_provider_create_provider_id_df.show(5, truncate=False)
                        bill_provider_create_provider_id_count = bill_provider_create_provider_id_df.count()
                        logger.info(f"Bill provider Create provider_id Record Count => {bill_provider_create_provider_id_count}")
                        logger.info(f"Calling update_db_records function to update {update_main_table_name} in database...")
                        update_db_records(bill_provider_create_provider_id_df, secrets, update_main_table_name, update_stage_table_name, query)

                        logger.info(f"Update invoice_status table...")
                        status_type = 'PROVIDER_MATCH_AUTO'
                        status = 'FOUND'
                        bill_provider_create_txm_id_df=temp_updated_bill_provider_create_df \
                            .withColumn("txm_invoice_number", explode(split(regexp_replace(col("txm_invoice_number_str"), "(^\[)|(\]$)", ""), ","))) \
                            .select("txm_invoice_number").distinct()
                        bill_provider_create_txm_id_df.printSchema()
                        bill_provider_create_txm_id_df.show(5, truncate=False)
                        bill_provider_create_txm_id_count = bill_provider_create_txm_id_df.count()
                        logger.info(f"Bill provider Create txm_invoice_number Record Count => {bill_provider_create_txm_id_count}")
                        update_invoice_status(bill_provider_create_txm_id_df, secrets, status_type, status)

                    #updated log table
                    db_sp_params = batch_id, None, None, None,'PROVIDER CREATE COMPLETED', 'Glue', None, None, 'Glue - ETL', 'Glue - ETL'
                    logger.info(f"db_sp_params => {db_sp_params}")
                    dbStoredProcedure = "txm_bitx_enrichment_logs.update_provider_request_log_status"
                    logger.info(f"dbStoredProcedure => {dbStoredProcedure}")
                    res = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, None)
                    logger.info(f"res => {res}")
                
                else:
                    logger.info(f"Not Found Provider create records...")

            else:
                if bill_data_count > 0:
                    logger.info(f"Skip the provider match flow for batch_id => {batch_id} and source type => {source}")

                    logger.info(f"Update invoice_status table...")
                    status_type = 'PROVIDER_MATCH_AUTO'
                    status = 'NOT_APPLICABLE'
                    update_invoice_status(bill_data_df, secrets, status_type, status)

                #updated log table
                db_sp_params = batch_id, None, None, None, 'PROVIDER MATCH SKIP FLOW COMPLETED', 'Glue', None, None, 'Glue - ETL', 'Glue - ETL'
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
    if is_skip_execution:
        status="COMPLETED SKIP REPROCESS"
    elif is_skip_no_data_error:
        status="COMPLETED SKIP NO DATA"
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
    logger.error(f'Error (Caught in Main Exception): {str(e)}')
    if is_provider_search:
        status="FAILED IN PROVIDER SEARCH"
        errorInfo="Error occured during Provider Search"

        logger.info(f"Update invoice_status table...")
        status_type = 'PROVIDER_MATCH_AUTO'
        status = 'FAILED'
        bill_data_df.printSchema()
        bill_data_df.show(5, truncate=False)
        update_invoice_status(bill_data_df, secrets, status_type, status)
    elif is_provider_create:
        status="FAILED IN PROVIDER CREATE"
        errorInfo="Error occured during Provider Create"

        logger.info(f"Update invoice_status table...")
        status_type = 'PROVIDER_MATCH_AUTO'
        status = 'FAILED'
        if is_create_batch:
            error_bill_provider_create_df = batch_bill_provider_create_df.filter(col("row_number") >= start_row)
        else:
            error_bill_provider_create_df = bill_provider_create_df
        bill_provider_create_txm_id_df=error_bill_provider_create_df \
            .withColumn("txm_invoice_number", explode(split(regexp_replace(col("txm_invoice_number_str"), "(^\[)|(\]$)", ""), ","))) \
            .select("txm_invoice_number").distinct()
        bill_provider_create_txm_id_df.printSchema()
        bill_provider_create_txm_id_df.show(5, truncate=False)
        update_invoice_status(bill_provider_create_txm_id_df, secrets, status_type, status)
    elif "CustomError: No Data Found in the Batch." in str(e):
        status="FAILED NO DATA"
        errorInfo="No Data found in the Batch"
    else:
        status="FAILED"
        errorInfo="Error occured during Provider Match"
    severity="FATAL"
    errormessage=str(e)
    logger.info("error in exception block")
    logger.info(errorInfo)
    
    response=log_and_email(env_profile,errorInfo,file_header_id,source,trading_partner,errormessage[:500],severity,sns_topic_arn)
    logger.info(f"Email sent successfully!message_id:{response['MessageId']}")
    raise
finally:
    #updated log table
    db_sp_params = batch_id, None, None, None, status, 'Glue', errorInfo, errormessage, 'Glue - ETL', 'Glue - ETL'
    logger.info(f"db_sp_params => {db_sp_params}")
    dbStoredProcedure = "txm_bitx_enrichment_logs.update_provider_request_log_status"
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