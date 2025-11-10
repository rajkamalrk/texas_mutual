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
from pyspark.sql.functions import lit, row_number, monotonically_increasing_id, col, lower, when
from pyspark.sql.window import Window

#Configure logging.
logging.basicConfig(level=logging.INFO)
#set the logging level to INFO
logger = logging.getLogger("claimant-billingestion-staging-load-job")

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

def archive_file(bucketName,archivekey,sourceKey):
     """Archive s3 file"""
     logger.info("inside method archiving with parameters")
     s3_client=boto3.client('s3')
     try:   
      logger.info("inside try of archiving method")
      s3_client.copy_object(Bucket=bucketName,CopySource={'Bucket':bucketName,'Key':sourceKey},Key=archivekey)
      logger.info(f"File archived to s3://{bucketName}/{archivekey}")
      s3_client.delete_object(Bucket=bucketName,Key=sourceKey)
     except Exception as e:
      logger.error(f"An error occured while archiving file :{e}")   
        
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

def get_df_db_data_with_filter_id(table_name, select_cols_list, filter_id_col_name, filter_id_col_value, load_timestamp, secrets):
    try:
        jdbc_url = f'jdbc:mysql://{secrets["host"]}:{secrets["port"]}/{secrets["dbClusterIdentifier"]}'
        addn_filter_str = ""
        if load_timestamp:
            addn_filter_str = f" AND create_timestamp = '{load_timestamp}'"
        read_db_sql = f"""(SELECT {select_cols_list} FROM {table_name} WHERE {filter_id_col_name} = {filter_id_col_value}{addn_filter_str}) AS db_vw"""
        logger.info(f"read_db_sql => {read_db_sql}")
        df_db_filter_res = spark.read.format("jdbc") \
                        .option("url", jdbc_url) \
                        .option("dbtable", read_db_sql) \
                        .option("user", secrets["username"]) \
                        .option("password", secrets["password"]) \
                        .option("numPartitions", "5") \
                        .option("fetchsize", "1000") \
                        .load()
        if df_db_filter_res.count() == 0:
            logger.info(f"No matching records found for Filter column {filter_id_col_name} -> '{filter_id_col_value}'")
        return df_db_filter_res
    except Exception as e:
        logger.error("Error executing DB reading: %s", str(e))

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

def log_and_email(env_profile,errorInfo,file_name,trading_partner,errormessage,severity,sns_topic_arn):
     """logging error and sending error email"""
     logger.info("inside log email method")
     cst=pytz.timezone('America/Chicago')
     timestamp=datetime.now(cst).strftime('%Y/%m/%d %H:%M:%S %Z%z')
     trading_partner=trading_partner.upper()
     #create log message
     process=f"Process : {trading_partner} SOLUTIONS INC - Extract eBill Metadata to load into Staging tables"
     errInfo=f"Error  : {errorInfo}"
     filenm=f"FileName : {file_name}"
     time=f"Timestamp : {timestamp}"
     errLog=f"Fields & Content from Error log : {errormessage}"
     log_message=f"{process}\n{errInfo}\n{filenm}\n{time}"
     logger.error(log_message)
     subject=f"{env_profile} - {trading_partner}  SOLUTIONS INC - {errorInfo} -{severity}"
     
     response=sns_client.publish(
        TopicArn=sns_topic_arn,
        Subject=subject,
        Message=f"{process}\n{errInfo}\n{filenm}\n{time}\n{errLog}"
        )
     return response

## @params: [claimant-billingestion-staging-load-job]
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'env_profile',
    's3_bucket',
    's3_input_files_prefix',
    'trading_partner',
    'staging_db_secret_key',
    's3_archive_prefix',
    's3_error_prefix',
    'datacatalog_schema_name',
    'datacatalog_config_table_name',
    'sns_topic_arn',
    'sqs_queue_url',
    'reprocess_flag',
    'sf_batch_id'
])

#Create GlueContext, SparkContext, and SparkSession
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job=Job(glueContext)
job.init(args['JOB_NAME'],args)
logger.info("Glue job initialized successfully...")
job_id = args['JOB_RUN_ID']
logger.info(f"jobid-{job_id}")

env_profile=args['env_profile']
s3_bucket=args['s3_bucket']
s3_input_files_prefix=args['s3_input_files_prefix']
trading_partner=args['trading_partner']
staging_db_secret_key=args['staging_db_secret_key']
s3_archive_prefix=args['s3_archive_prefix']
s3_error_prefix=args['s3_error_prefix']
datacatalog_schema_name=args['datacatalog_schema_name']
datacatalog_config_table_name=args['datacatalog_config_table_name']
sns_topic_arn=args['sns_topic_arn']
sqs_queue_url=args['sqs_queue_url']
reprocess_flag=args.get('reprocess_flag', 'N').upper()  # Default to N if not present
sf_batch_id=args['sf_batch_id']

sns_client=boto3.client('sns',region_name='us-east-1')
#Initializations
is_skip_execution = False
is_no_data_execution = False
log_status = ""
archive_key = ""
mode = "append"
s3_path=f"s3://{s3_bucket}/{s3_input_files_prefix}/"
logger.info(f"s3_path => {s3_path}")
file_name=s3_input_files_prefix.split('/')[-1]
logger.info(f"file name:{file_name}")

#Replacing trading partner in archive and error location
logger.info(f"s3_archive_prefix => {s3_archive_prefix}")
s3_archive_prefix = s3_archive_prefix.replace("<TRADING_PARTNER>",trading_partner)
logger.info(f"NEW s3_archive_prefix => {s3_archive_prefix}")

logger.info(f"s3_error_prefix => {s3_error_prefix}")
s3_error_prefix = s3_error_prefix.replace("<TRADING_PARTNER>",trading_partner)
logger.info(f"NEW s3_error_prefix => {s3_error_prefix}")

#Custom exception classes
class S3AccessError(Exception):
    pass
class FileNotFoundError(Exception):
    pass

spark.conf.set("spark.sql.ansi.enabled","True")

try:
    #logging data in to table logs
    logger.info(f"staging_db_secret_key => {staging_db_secret_key}")
    secrets = get_secret(staging_db_secret_key, 'us-east-1')
    
    file_header_id = None

    db_sp_params = trading_partner, None, file_name, s3_path, None, job_id, 'Glue - ETL - Staging Load', 'Glue - ETL - Staging Load', 0
    logger.info(f"db_sp_params => {db_sp_params}")
    dbStoredProcedure = "txm_bitx_staging_logs.insert_staging_logs"
    logger.info(f"dbStoredProcedure => {dbStoredProcedure}")
    batch_id = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, 8)
    logger.info(f"batch_id => {batch_id}")
    
    logger.info(f"reprocess_flag => {reprocess_flag}")
    
    # Read logtable records from db with filtered file_name and trading_partner
    read_db_sql = f"""
        SELECT * FROM txm_bitx_staging_logs.batch_staging_details 
        WHERE trading_partner = '{trading_partner}'
        AND file_name = '{file_name}'
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
            if log_status.startswith('COMPLETED'):
                is_skip_execution = True
                file_header_id = existing_log_df.first()["file_header_id"]
                logger.info(f"Existing entry found for trading partner {trading_partner}, File Name {file_name} and File Header ID {file_header_id} with status as {log_status} for reprocessing flow. No further processing required, skipping rest of the flow!!!")
            else:
                logger.info(f"Existing entry found for trading partner {trading_partner} and File Name {file_name} with status as {log_status} for reprocessing flow. Reprocessing flow started...")
        else:
            #Error Flow
            error_message = f"CustomError: Duplicate Batch, Log entry already exists for trading partner {trading_partner} and File Name {file_name} with reprocess_flag as {reprocess_flag} (not as Y)."
            logger.error(f"error_message => {error_message}")
            raise Exception(error_message)
    else:
        logger.info(f"No existing entry found for trading partner {trading_partner} and File Name {file_name}. Proceeding with staging load...")

    if not is_skip_execution:
        logger.info(f"Processing STARTED for trading partner {trading_partner}...")
        #Reading data file from S3
        logger.info(f"Reading data file for trading partner {trading_partner} from S3...")
        init_df=spark.read.text(s3_path).toDF("bill_data_str")
        init_df.printSchema()
        init_df.show(5, truncate=False)
        file_record_count = init_df.count()
        logger.info(f"Data File Record Count => {file_record_count}")

        if file_record_count > 0:
            logger.info(f"Reading Bill Ingestion configuration from Data Catalog table => {datacatalog_schema_name}.{datacatalog_config_table_name}...")
            AWSGlueDataCatalog_StagingConfig = glueContext.create_dynamic_frame.from_catalog(database=datacatalog_schema_name, table_name=datacatalog_config_table_name, transformation_ctx="AWSGlueDataCatalog_StagingConfig")
            AWSGlueDataCatalog_StagingConfig.printSchema()
            AWSGlueDataCatalog_StagingConfig.show()
            config_df = AWSGlueDataCatalog_StagingConfig.toDF()
            config_rec = config_df.filter(lower(col("trading_partner")) == trading_partner.lower()).first()
            logger.info(f"config_rec => {config_rec}")

            if config_rec["require_parsing"] == "Yes":
                logger.info(f"Started with pre-processing for {trading_partner}...")
                init_df = init_df.withColumn("bill_index_no", row_number().over(Window.orderBy(monotonically_increasing_id())))
                init_df.printSchema()
                init_df.show(5, truncate=False)
                init_df.createOrReplaceTempView(config_rec["df_table_name"])
                init_transform_df = spark.sql(config_rec["transform_sql_select"])
                init_transform_df.printSchema()
                init_transform_df.show(5, truncate=False)
                recordCount = init_transform_df.count()
                logger.info(f"Data File Record Count => {recordCount}")
            else:
                logger.info(f"No pre-processing required for {trading_partner}...")
                init_transform_df = init_df

            logger.info(f'Reading {trading_partner} configuration from Data Catalog table => {config_rec["datacatalog_schema_name"]}.{config_rec["datacatalog_table_name"]}...')
            AWSGlueDataCatalog_StagingTPConfig = glueContext.create_dynamic_frame.from_catalog(database=config_rec["datacatalog_schema_name"], table_name=config_rec["datacatalog_table_name"], transformation_ctx="AWSGlueDataCatalog_StagingTPConfig")
            AWSGlueDataCatalog_StagingTPConfig.printSchema()
            AWSGlueDataCatalog_StagingTPConfig.show()
            tp_config_df = AWSGlueDataCatalog_StagingTPConfig.toDF()
            tp_config_df.show(3, truncate=False)
            recordCount = tp_config_df.count()
            logger.info(f"Trading Partner config Record Count => {recordCount}")
            tp_config_itr = tp_config_df.orderBy(['order_seq_no']).rdd.toLocalIterator()

            #Processing Trading Partner record type configuration in order sequence
            for row in tp_config_itr:
                logger.info(f"Processing started for row: {row}")
                v_load_timestamp = None
                v_filter_id_col_value = None

                #Filtering each table data based on first two characters and loaded into a dataframe
                record_type_data_df = init_transform_df.filter(col("bill_data_str").substr(1, len(row["record_type_code"])) == row["record_type_code"])
                
                record_type_data_df.createOrReplaceTempView(row["df_table_name"])
                record_type_data_transform_df = spark.sql(row["transform_sql_select"])
                record_type_data_transform_df.printSchema()
                record_type_data_transform_df.show(5, truncate=False)
                recordCount = record_type_data_transform_df.count()
                logger.info(f"Record Type Data Transform Record Count => {recordCount}")

                # Convert String columns with empty values to NULL
                for col_name in record_type_data_transform_df.columns:
                    if dict(record_type_data_transform_df.dtypes)[col_name] in ["string"]:
                        record_type_data_transform_df = record_type_data_transform_df.withColumn(col_name, when(col(col_name)=="" ,None).otherwise(col(col_name)))
                record_type_data_transform_df.printSchema()
                record_type_data_transform_df.show(5, truncate=False)
                recordCount = record_type_data_transform_df.count()
                logger.info(f"NEW Record Type Data Transform Record Count => {recordCount}")

                logger.info(f"Adding ETL processing columns...")
                #2024-06-09 19:29:19.746
                v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                logger.info(f"v_load_timestamp => {v_load_timestamp}")
                record_type_data_transform_df = record_type_data_transform_df \
                    .withColumn("create_user", lit("Glue ETL - Staging load")) \
                    .withColumn("update_user", lit("Glue ETL - Staging load")) \
                    .withColumn("create_timestamp", lit(v_load_timestamp)) \
                    .withColumn("update_timestamp", lit(v_load_timestamp))
                record_type_data_transform_df.printSchema()
                record_type_data_transform_df.show(1, truncate=False)
                recordCount = record_type_data_transform_df.count()
                logger.info(f"Record Type Data Transform Record Count => {recordCount}")

                logger.info(f'Load transformed dataframe to RDS table => {row["db_table_name"]}')
                load_data_to_rds(record_type_data_transform_df, row["db_table_name"], secrets, mode)

                logger.info(f'record_type => {row["record_type"]}')
                if row["record_type"] in ("FILE_HEADER","BILL_HEADER","INVOICE_HEADER"):
                    logger.info(f"Record Type is File or Bill Header - extracting generated id(s)...")
                    logger.info(f'db_filter_id_col_name => {row["db_filter_id_col_name"]}')
                    v_filter_id_col_value = record_type_data_transform_df.first()[row["db_filter_id_col_name"]]
                    logger.info(f"v_filter_id_col_value => {v_filter_id_col_value}")
                    if dict(record_type_data_transform_df.dtypes)[row["db_filter_id_col_name"]] not in ["int","bigint"]:
                        v_filter_id_col_value = f"'{v_filter_id_col_value}'"
                        logger.info(f"v_filter_id_col_value => {v_filter_id_col_value}")
                    if row["record_type"] != "FILE_HEADER":
                        v_load_timestamp = None
                    logger.info(f"v_load_timestamp => {v_load_timestamp}")
                    db_filter_id_df = get_df_db_data_with_filter_id(row["db_table_name"], row["db_select_cols_list"], row["db_filter_id_col_name"], v_filter_id_col_value, v_load_timestamp, secrets)
                    db_filter_id_df = db_filter_id_df.cache()
                    db_filter_id_df.createOrReplaceTempView("db_" + row["df_table_name"])
                    db_filter_id_df.show(5, truncate=False)
                    recordCount = db_filter_id_df.count()
                    logger.info(f"DB Filter IDs Record Count => {recordCount}")
                    if row["record_type"] == "FILE_HEADER":
                        if "file_header_id" in db_filter_id_df.columns:
                            file_header_id = db_filter_id_df.first()["file_header_id"]
                            logger.info(f'file_header_id => {file_header_id}')
                        else:
                            logger.info(f'file_header_id NOT exists in FILE HEADER table!!!')

            #Archiving on successfull process   
            archive_key=f"{s3_archive_prefix}/{file_name}"
        else:
            is_no_data_execution = True
            logger.info(f"No Data Avaialble for trading partner {trading_partner} and File Name {file_name}. Skipping the rest of the flow!!!")
    else:
        logger.info(f"Existing entry found for trading partner {trading_partner}, File Name {file_name} and File Header ID {file_header_id} with status as {log_status} for reprocessing flow. No further processing required, skipping rest of the flow!!!")
    
    if not is_no_data_execution:
        #updated log table - Step Function logs
        db_sp_params = sf_batch_id, file_header_id, None, None, None, None, None, None ,None, None, None, 'STAGING', 'COMPLETED',  'Glue - ETL - Staging Load'
        logger.info(f"db_sp_params => {db_sp_params}")
        dbStoredProcedure = "txm_bitx_logs.update_step_function_log_status"
        logger.info(f"dbStoredProcedure => {dbStoredProcedure}")
        res = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, None)
        logger.info(f"res => {res}")

        # Create SQS client
        sqs_client = boto3.client('sqs')
        
        # Create the message
        output_sqs_message = {
            'source': 'ebill',
            'trading_partner': trading_partner,
            'file_header_id': file_header_id,
            'reprocess_flag': reprocess_flag,
            'sf_batch_id': sf_batch_id
        }
        logger.info(f'sqs_queue_url => {sqs_queue_url}')
        logger.info(f'output_sqs_message => {output_sqs_message}')

        #updated log table - Step Function logs
        db_sp_params = sf_batch_id, file_header_id, None, None, None, None, None, None ,json.dumps(output_sqs_message), None, None, 'INVOKE ENHANCED', 'IN PROGRESS',  'Glue - ETL - Staging Load'
        logger.info(f"db_sp_params => {db_sp_params}")
        dbStoredProcedure = "txm_bitx_logs.update_step_function_log_status"
        logger.info(f"dbStoredProcedure => {dbStoredProcedure}")
        res = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, None)
        logger.info(f"res => {res}")

        try:
            # Send the message to SQS
            sqs_response = sqs_client.send_message(
                QueueUrl=sqs_queue_url,
                MessageBody=json.dumps(output_sqs_message)
            )
            print(f'Message sent to SQS with response: {sqs_response}')
        
        except Exception as e:
            print(f'Error sending message to SQS: {e}')
            # Handle error gracefully, optionally raise or log it
            raise e

    #Data for log table
    if is_skip_execution:
        status="COMPLETED SKIP REPROCESS"
    elif is_no_data_execution:
        status="FAILED NO DATA"
        #Error Flow
        error_message = f"CustomError: Data NOT Available in Input File for trading partner {trading_partner} and File Name {file_name}."
        logger.error(f"error_message => {error_message}")
        raise Exception(error_message)
    else:
        status="COMPLETED"
    errorInfo="None"
    errormessage="None"
    logger.info(f"Processing COMPLETED for trading partner {trading_partner}, File Name {file_name} and File Header ID {file_header_id}!!!")

except S3AccessError as e:
    logger.error(e)
    raise
except FileNotFoundError as e:
    logger.error(e)
    raise
except Exception as e:
    if "NoSuchBucket" in str(e):
        logger.error(f"Cannot access bucket:{s3_bucket}. The bucket does not exist or is not accessible.")
        raise S3AccessError(f"Cannot access bucket:{s3_bucket}. The bucket does not exist or is not accessible.")
    elif "Path does not exist" in str(e):
        status="FAILED"
        errorInfo="eBill file not found in S3 location"
        errormessage=f"Cannot find file:{s3_path}. The file does not exist in the specified bucket/prefix"
        response=log_and_email(env_profile,errorInfo,file_name,trading_partner,errormessage,"FATAL",sns_topic_arn)
        logger.info(f"Email sent successfully!message_id:{response['MessageId']}")
        raise FileNotFoundError(errormessage)
    else:
        archive_key=f"{s3_error_prefix}/{file_name}"
        if "CustomError: Duplicate Batch" in str(e):
            status="FAILED DUPLICATE BATCH"
            errorInfo="Duplicate Batch"
        elif "CustomError: Data NOT Available in Staging" in str(e):
            status="FAILED NO DATA"
            errorInfo="Data NOT Available in Staging"
        elif "CustomError: All Bills Validation FAILED in the Batch" in str(e):
            status="FAILED ALL BILLS"
            errorInfo="All Bills Validation FAILED in the Batch"
        else:
            status="FAILED"
            errorInfo="Unable to process 1 or more records"
        severity="FATAL"
        logger.info(str(e))
        #errormessage=re.search(r"(.+?Exception:.+?)(?=\n|$)",str(e)).group(1)
        errormessage=str(e)[:500]
        response=log_and_email(env_profile,errorInfo,file_name,trading_partner,errormessage,severity,sns_topic_arn)
        logger.info(f"Email sent successfully!message_id:{response['MessageId']}")
        raise
finally:
    #updated log table
    db_sp_params = batch_id, file_header_id, status, 'Glue', errorInfo, errormessage, 'Glue - ETL - Staging Load', 'Glue - ETL - Staging Load'
    logger.info(f"db_sp_params => {db_sp_params}")
    dbStoredProcedure = "txm_bitx_staging_logs.update_staging_log_status"
    logger.info(f"dbStoredProcedure => {dbStoredProcedure}")
    res = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, None)
    logger.info(f"res => {res}")
    logger.info("Calling archive function...")
    archive_file(bucketName=s3_bucket,archivekey=archive_key,sourceKey=s3_input_files_prefix)
    logger.info("Completed archive!!!")
try:
    #Commit the job
    job.commit()
    logger.info("Job committed successfully")
except Exception as e:
    logger.error(f"Error committing the job:{e}")
    raise