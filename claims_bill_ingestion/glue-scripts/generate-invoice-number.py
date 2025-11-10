import sys
import logging
import json
import base64
import boto3
import pymysql
import pytz
from datetime import datetime
from botocore.exceptions import ClientError
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit, row_number, col
from pyspark.sql.window import Window

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("claimant-billingestion-generate-invoice-number-job")

# Parameters for the job
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 
    'env_profile', 
    'source', 
    'trading_partner', 
    'batch_size', 
    'file_header_id',
    'invoice_num_generate_lambda_function', 
    'db_secret_key',
    'sns_topic_arn',
    'reprocess_flag'
    ])

# Configuration
env_profile = args['env_profile']
source = args['source']
trading_partner = args['trading_partner']
file_header_id = args['file_header_id']
batch_size = int(args['batch_size'])
invoice_num_generate_lambda_function = args['invoice_num_generate_lambda_function']
db_secret_key = args['db_secret_key']
sns_topic_arn = args['sns_topic_arn']
reprocess_flag=args.get('reprocess_flag', 'N').upper()  # Default to N if not present


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

def request_invoices_numbers(batch_size):
    client = boto3.client('lambda',region_name='us-east-1')
    invoice_number_list = []

    payload = {
        "numIncrements": batch_size
    }
    logger.info(f"Payload to Lambda: {json.dumps(payload)}")
    
    try:
        response = client.invoke(
            FunctionName=invoice_num_generate_lambda_function,
            InvocationType='RequestResponse',
            Payload=json.dumps(payload)
        )
   
        response_payload = response['Payload'].read().decode('utf-8')
        logger.info(f"Lambda response payload: {response_payload}")
        if isinstance(response_payload, str):
            response_payload = json.loads(response_payload)
        logger.info(f"response_payload => {response_payload}")

        #Check if the response is not None and contains the required data
        if response_payload and 'body' in response_payload:
            response_body_payload = response_payload['body']
            if isinstance(response_body_payload, str):
                response_body_payload = json.loads(response_body_payload)
            if 'status' in response_body_payload and response_body_payload['status'] == 'success' and 'data' in response_body_payload:
                if batch_size == 1:
                    invoice_number = response_body_payload['data'].get('invoiceNumber')
                    if invoice_number:
                        invoice_number_list = [invoice_number]
                    else:
                        invoice_number_list = []
                        logger.error("Failed to retrieve single Invoice number from Lambda response.")
                        raise Exception(f"Failed to retrieve single Invoice number from Lambda response. {response_payload}")
                else:
                    # Handle multiple invoiceNumber case
                    invoice_number_list = response_body_payload['data'].get('invoiceNumber')
                    if not invoice_number_list:
                        invoice_number_list = []
                        logger.error("Failed to retrieve Invoice numbers from Lambda response.")
                        raise Exception(f"Failed to retrieve Invoice numbers from Lambda response. {response_payload}")
            else:
                logger.error("Lambda response Body not in expected format.")
                raise Exception(f"Lambda response Body not in expected format. {response_payload}")
        else:
            logger.error("Lambda response not in expected format.")
            raise Exception(f"Lambda response not in expected format. {response_payload}")
    
        return invoice_number_list

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
     if trading_partner and trading_partner.strip() and trading_partner.lower() != "null":
        source_tp_str = trading_partner
     else:
        source_tp_str = source
     source_tp_str = source_tp_str.upper()
     #create log message
     process=f"Process : {source_tp_str} SOLUTIONS INC - Generate Invoice Number"
     errInfo=f"Error  : {errorInfo}"
     fileid=f"File Header ID (TxM Staging) : {file_header_id}"
     #billid=f"Bill ID (TxM Staging) : {bill_header_id}"
     time=f"Timestamp : {timestamp}"
     errLog=f"Fields & Content from Error log : {errormessage}"
     log_message=f"{process}\n{errInfo}\n{fileid}\n{time}"
     logger.error(log_message)
     subject=f"{env_profile} - {source_tp_str}  SOLUTIONS INC - {errorInfo} -{severity}"
     
     response=sns_client.publish(
        TopicArn=sns_topic_arn,
        Subject=subject,
        Message=f"{process}\n{errInfo}\n{fileid}\n{time}\n{errLog}"
        )
     return response    

#Create GlueContext, SparkContext, and SparkSession
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job=Job(glueContext)
job.init(args['JOB_NAME'],args)
logger.info("Glue job initialized successfully...")
job_id = args['JOB_RUN_ID']
logger.info(f"jobid-{job_id}")

#Initializations
batch_id=None
is_skip_execution = False
is_no_data_execution = False
log_status = ""
mode = "append"

try:
    logger.info(f"Processing STARTED for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id}...")

    logger.info(f"db_secret_key => {db_secret_key}")
    secrets = get_secret(db_secret_key, 'us-east-1')

    db_sp_params = source,trading_partner, file_header_id, job_id, 'Glue - ETL', 'Glue - ETL', 0
    logger.info(f"db_sp_params => {db_sp_params}")
    dbStoredProcedure = "txm_bitx_enrichment_logs.insert_generate_invoice_logs"
    logger.info(f"dbStoredProcedure => {dbStoredProcedure}")
    batch_id = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, 6)
    logger.info(f"batch_id => {batch_id}")

    logger.info(f"reprocess_flag => {reprocess_flag}")

    read_db_sql = f"""
    SELECT * FROM txm_bitx_enrichment_logs.batch_generate_invoice_master 
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
        logger.info(f"No existing entry found for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id}. Proceeding with generate invoice...")
    
    if not is_skip_execution:
        if source.lower() == "ebill":
            read_db_sql = f"""
                SELECT b.bill_id
                FROM txm_bitx.bill_header b
                LEFT JOIN txm_bitx.invoice_status s ON b.txm_invoice_number = s.txm_invoice_number
                WHERE b.source = '{source}'
                AND b.trading_partner = '{trading_partner}' 
                AND b.file_header_id = {file_header_id}
                AND (b.txm_invoice_number IS NULL OR s.txm_invoice_number IS NULL)
            """

            bills_to_process_df = get_df_db_data_with_query(read_db_sql, secrets)
            bills_to_process_df = bills_to_process_df.cache()
            bills_to_process_df.printSchema()
            bills_to_process_df.show(5, truncate=False)
            bill_list_count = bills_to_process_df.count()
            logger.info(f"Bill List Record Count => {bill_list_count}")

            if bill_list_count > 0:
                # Batch processing
                window_spec = Window.orderBy("bill_id")
                bills_to_process_df = bills_to_process_df.withColumn("row_number", row_number().over(window_spec))
                bills_to_process_df.cache()
                bills_to_process_df.printSchema()
                bills_to_process_df.show(5, truncate=False)
                bills_record_count = bills_to_process_df.count()
                logger.info(f"Bills To Process with Row NUmber Record Count => {bills_record_count}")

                # Process each batch
                logger.info(f"Invoice Generation batch based on batch_size {batch_size}...")
                for start_row in range(1, bills_record_count + 1, batch_size):
                    end_row = min(start_row + batch_size - 1, bills_record_count)
                    logger.info(f"Processing batch with start row {start_row} and end row {end_row}...")
                    batch_df = bills_to_process_df.filter((col("row_number") >= start_row) & (col("row_number") <= end_row)).cache()
                    batch_df.printSchema()
                    batch_df.show(5, truncate=False)
                    batch_record_count = batch_df.count()
                    logger.info(f"Batch DF Record Count => {batch_record_count}")

                    # Check if data available for Batch
                    if batch_record_count > 0:
                        logger.info(f"Data Available for start row {start_row} and end row {end_row}...")
                    else:
                        #Error Flow                   
                        error_message = f"CustomError: Data NOT Available start row {start_row} and end row {end_row}."
                        logger.error(f"error_message => {error_message}")
                        raise Exception(error_message)
                        
                    invoice_numbers_list = request_invoices_numbers(batch_record_count)
                    if invoice_numbers_list:                
                        # Extract the invoice number from the parsed response
                        logger.info(f"INVOICE NUMBER list :{invoice_numbers_list}")
                        if batch_record_count == 1:
                            invoice_number = [{"txm_invoice_number": item, "row_number": start_row + idx} for idx, item in enumerate(invoice_numbers_list)]
                        else:
                            invoice_number = [{"txm_invoice_number": item['invoice_number'], "row_number": start_row + idx} for idx, item in enumerate(invoice_numbers_list)]
                        logger.info(f"INVOICE NUMBER :{invoice_number}")

                        df_updated_invoice_number = spark.createDataFrame(invoice_number)
                        df_updated_invoice_number.printSchema()
                        df_updated_invoice_number.show(5, truncate=False)
                        recordCount = df_updated_invoice_number.count()
                        logger.info(f"Invoice Number Generated Record Count => {recordCount}")

                        batch_combined_df = batch_df.join(df_updated_invoice_number,"row_number")
                        batch_combined_df.printSchema()
                        batch_combined_df.show(5, truncate=False)
                        recordCount = batch_combined_df.count()
                        logger.info(f"Batch Combined Record Count => {recordCount}")

                        update_main_table_name = f"txm_bitx.bill_header"
                        logger.info(f"update_main_table_name => {update_main_table_name}")

                        update_stage_table_name = f"{update_main_table_name}_{trading_partner}_{file_header_id}_stage"
                        logger.info(f"update_stage_table_name => {update_stage_table_name}")
                        v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

                        query = f"""
                            UPDATE {update_main_table_name} main
                            JOIN {update_stage_table_name} stage ON main.bill_id = stage.bill_id
                            SET main.txm_invoice_number = stage.txm_invoice_number,
                            main.update_user = 'Glue - ETL - Generate invoice',
                            main.update_timestamp = '{v_load_timestamp}'
                        """

                        logger.info(f"Calling update_db_records function to update {update_main_table_name} in database...")
                        update_db_records(batch_combined_df, secrets, update_main_table_name, update_stage_table_name, query)

                        invoice_status_type='ENRICHMENT'
                        invoice_status='OPEN'
                        v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                        df_invoice_status=batch_combined_df \
                            .withColumn('status_type',lit(invoice_status_type)) \
                            .withColumn('status',lit(invoice_status)) \
                            .withColumn('create_user',lit('Glue - ETL')) \
                            .withColumn('update_user',lit('Glue - ETL')) \
                            .withColumn('create_timestamp',lit(v_load_timestamp)) \
                            .withColumn('update_timestamp',lit(v_load_timestamp)) \
                            .drop('bill_id') \
                            .drop('row_number')
                        df_invoice_status.printSchema()
                        df_invoice_status.show(5, truncate=False)
                        recordCount = df_invoice_status.count()
                        logger.info(f"Invoice Status Record Count => {recordCount}")

                        logger.info(f"Inserting txm_invoice_numbers to invoice_status table in database...")
                        load_data_to_rds(df_invoice_status, 'txm_bitx.invoice_status', secrets, 'append')
                    else:
                        error_message=f"Error fetching invoice numbers."
                        logger.error(f"error_message => {error_message}")
                        raise Exception(error_message)
            else:
                is_no_data_execution = True
                logger.info(f"No Data Available for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id}. Skipping the rest of the flow!!!")

        else:
            read_db_sql = f"""
                SELECT b.bill_id,b.txm_invoice_number
                FROM txm_bitx.bill_header b
                LEFT JOIN txm_bitx.invoice_status s ON b.txm_invoice_number = s.txm_invoice_number
                WHERE b.source = '{source}'
                AND b.trading_partner IS NULL
                AND b.file_header_id = {file_header_id}
                AND b.txm_invoice_number IS NOT NULL AND s.txm_invoice_number IS NULL
            """

            bills_to_process_df = get_df_db_data_with_query(read_db_sql, secrets)
            bills_to_process_df = bills_to_process_df.cache()
            bills_to_process_df.printSchema()
            bills_to_process_df.show(5, truncate=False)
            bill_list_count = bills_to_process_df.count()
            logger.info(f"Bill List Record Count => {bill_list_count}")

            if bill_list_count > 0:
                logger.info(f"loading invoice_status table for source: {source}")

                invoice_status_type='ENRICHMENT'
                invoice_status='OPEN'
                v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                df_invoice_status=bills_to_process_df \
                    .withColumn('status_type',lit(invoice_status_type)) \
                    .withColumn('status',lit(invoice_status)) \
                    .withColumn('create_user',lit('Glue - ETL')) \
                    .withColumn('update_user',lit('Glue - ETL')) \
                    .withColumn('create_timestamp',lit(v_load_timestamp)) \
                    .withColumn('update_timestamp',lit(v_load_timestamp)) \
                    .drop('bill_id')
                df_invoice_status.printSchema()
                df_invoice_status.show(5, truncate=False)
                recordCount = df_invoice_status.count()
                logger.info(f"Invoice Status Record Count => {recordCount}")

                logger.info(f"Inserting txm_invoice_numbers to invoice_status table in database...")
                load_data_to_rds(df_invoice_status, 'txm_bitx.invoice_status', secrets, mode)

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
    if "CustomError: No Data Found in the Batch." in str(e):
        status="FAILED NO DATA"
        errorInfo="No Data found in the Batch"
    else:
        status="FAILED"
        errorInfo="Error occured while batch processing"
    
    severity="FATAL"
    errormessage=str(e)[:500]
    logger.info("error in exception block")
    logger.info(errorInfo)  
    logger.error(str(e))
    #errorInfo="Unable to process 1 or more Bill(s)"
    #errorInfo="Failed Generate Invoice"
    response=log_and_email(env_profile,errorInfo,file_header_id,source,trading_partner,errormessage,severity,sns_topic_arn)
    logger.info(f"Email sent successfully!message_id:{response['MessageId']}")
    #status = 'FAILED'
    raise
finally:
    #updated log table
    db_sp_params = batch_id,file_header_id,status,errorInfo, errormessage, 'Glue - ETL', 'Glue - ETL'
    logger.info(f"db_sp_params => {db_sp_params}")
    dbStoredProcedure = "txm_bitx_enrichment_logs.update_generate_invoice_log_status"
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