import sys
import logging
import json
import base64
import boto3
import pytz
import pymysql
import time
import builtins
from datetime import datetime
from botocore.exceptions import ClientError
from botocore.client import Config
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit, col, lower, udf, when, expr, concat, count, round
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

#Configure logging.
logging.basicConfig(level=logging.INFO)
#set the logging level to INFO
logger = logging.getLogger("claimant-billingestion-enhanced-load-job")

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
    connection=pymysql.connect(host=secrets['host'],user=secrets['username'],password=secrets['password'],database=secrets['dbClusterIdentifier'])
    try:
        with connection.cursor() as cursor:
            cursor.execute(db_sql)
            connection.commit()
    finally:
        connection.close()
        
def clean_claim_data_tables(table_name, is_child, main_table, trading_partner, file_header_id, secrets):
    if is_child:
        delete_script = f"""
        DELETE t
        FROM {table_name} t
        JOIN {main_table} m ON m.claim_data_id = t.claim_data_id
        WHERE m.trading_partner = '{trading_partner}'
        AND m.file_header_id = {file_header_id}
        """
    else:
        delete_script = f"""
        DELETE FROM {table_name}
        WHERE trading_partner = '{trading_partner}'
        AND file_header_id = {file_header_id}
        """
    logger.info(f"Deleting partial data from table => {table_name}")
    logger.info(delete_script)
    run_sql_query(delete_script, secrets)
    logger.info(f"Successfully deleted partial data from table => {table_name}")

def get_df_db_data_with_filter_id(source, trading_partner, table_name, table_type, select_cols_list, filter_id_col_name, filter_id_col_value, bill_header_table_name, secrets):
    try:
        jdbc_url = f'jdbc:mysql://{secrets["host"]}:{secrets["port"]}/{secrets["dbClusterIdentifier"]}'
        if table_type == "BILL_HEADER":
            read_db_sql = f"""(SELECT {select_cols_list} FROM {table_name} WHERE source = '{source}' AND trading_partner = '{trading_partner}' AND {filter_id_col_name} = {filter_id_col_value}) AS db_vw"""
        elif filter_id_col_name == "file_header_id" and table_type != "BILL_HEADER":
            read_db_sql = f"""(SELECT {select_cols_list.replace('bill_id','b.bill_id')} FROM {table_name} l JOIN {bill_header_table_name} b ON l.bill_id = b.bill_id WHERE b.source = '{source}' AND b.trading_partner = '{trading_partner}' AND {filter_id_col_name} = {filter_id_col_value}) AS db_vw"""
        else:
            read_db_sql = f"""(SELECT {select_cols_list} FROM {table_name} WHERE {filter_id_col_name} = {filter_id_col_value}) AS db_vw"""
        logger.info(f"read_db_sql => {read_db_sql}")
        df_db_filter_res = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", read_db_sql) \
             .option("user", secrets["username"]).option("password", secrets["password"]).load()
        if df_db_filter_res.count() == 0:
            logger.info(f"No matching records found for Filter column {filter_id_col_name} -> '{filter_id_col_value}'")
        return df_db_filter_res
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

def log_and_email(env_profile,errorInfo,file_header_id,bill_header_id,trading_partner,errormessage,severity,sns_topic_arn):
     """logging error and sending error email"""
     logger.info("inside log email method")
     sns_client=boto3.client('sns',region_name='us-east-1')
     cst=pytz.timezone('America/Chicago')
     timestamp=datetime.now(cst).strftime('%Y/%m/%d %H:%M:%S %Z%z')
     trading_partner=trading_partner.upper()
     #create log message
     process=f"Process : {trading_partner} SOLUTIONS INC - Extract, Validate and Transform eBill Metadata to load into Enhanced tables"
     errInfo=f"Error  : {errorInfo}"
     fileid=f"File Header ID (TxM Staging) : {file_header_id}"
     billid=f"Bill ID (TxM Staging) : {bill_header_id}"
     time=f"Timestamp : {timestamp}"
     errLog=f"Fields & Content from Error log : {errormessage}"
     log_message=f"{process}\n{errInfo}\n{fileid}\n{billid}\n{time}"
     logger.error(log_message)
     subject=f"{env_profile} - {trading_partner}  SOLUTIONS INC - {errorInfo} -{severity}"
     
     response=sns_client.publish(
        TopicArn=sns_topic_arn,
        Subject=subject,
        Message=f"{process}\n{errInfo}\n{fileid}\n{billid}\n{time}\n{errLog}"
        )
     return response
     
def log_and_email_attachment(process,env_profile, file_header_id,batch_id, trading_partner, bucket_name, source_prefix, target_prefix,file_name):
    logger.info("inside log email attachment method")
    try:
        current_date = datetime.now().strftime('%Y-%m-%d')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        cst_time = datetime.now(pytz.timezone('America/Chicago')).strftime('%Y/%m/%d %H:%M:%SCST')
        source_prefix_with_date = f"{source_prefix}/{current_date}/"
        target_prefix_with_date = f"{target_prefix}/{current_date}/"
        logger.info(f'source_prefix_with_date : {source_prefix_with_date}')
        logger.info(f'target_prefix_with_date : {target_prefix_with_date}')

        validation_error_mail_df = validation_error_df.withColumn('Error Log ID',lit(batch_id)) \
            .withColumn('TXM Invoice Number',lit('')) \
            .withColumn('Process',lit(process)) \
            .withColumn('TimeStamp',lit(cst_time)) \
            .withColumnRenamed('bill_header_id','Staging Bill ID') \
            .withColumnRenamed('validation_error_message','Error') \
            .withColumnRenamed('unique_bill_id','Trading Partner Bill ID') \
            .withColumnRenamed('field_name_1','Field name 1') \
            .withColumnRenamed('field_value_1','Field Value 1') \
            .withColumnRenamed('field_name_2','Field Name 2') \
            .withColumnRenamed('field_value_2','Field Value 2') \
            .select(col('Trading Partner Bill ID'),col('Staging Bill ID'),col('Error Log ID'),col('TXM Invoice Number'),col('Process'),col('Error'),col('Field Name 1'),col('Field Value 1'),col('Field Name 2'),col('Field Value 2'),col('TimeStamp'))

        validation_error_mail_df.printSchema()
        validation_error_mail_df.show(10, truncate=False)
        recordCount = validation_error_mail_df.count()
        logger.info(f"Validation Error Mail Record Count => {recordCount}")

        validation_error_mail_df.coalesce(1).write.option('header','true').mode('append').format('csv').save(f"s3://{bucket_name}/{source_prefix_with_date}")
        trading_partner=trading_partner.upper()
        s3_client = boto3.client('s3','us-east-1')
        target_file_name = f'{trading_partner}_{file_header_id}_{process}_{timestamp}.csv'
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
                    trading_partner= f'{trading_partner}  SOLUTIONS INC'
                else:
                    trading_partner=trading_partner

                # Invoke the Lambda function to send an email with the attachment
                client = boto3.client('lambda',region_name='us-east-1')
                payload = {
                    's3Bucket': bucket_name,
                    's3Key': target_file,
                    'to': error_alerts_to_email_ids,
                    'subject': f'{env_profile} - {trading_partner} - {process}',
                    'html': f"""<p>Please review the attached CSV for the errors encountered during {process}</p>
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

def calc_check_digit(npi):
    """calculating check digit for validate NPI UDF"""
    mlt=[2,1,2,1,2,1,2,1,2]
    result=24
    for i in range(9):
        work=int(npi[i])*mlt[i]
        result+=work//10+work%10
    if result%10==0:
        check_digit=0  
    else:
        new_result=result+10   #increment 10th digit by1
        new_result=(new_result//10)*10  #initialize unit digit to 0
        check_digit=new_result-result
    return check_digit 

def invoke_claim_match(bill_header_id,trading_partner,file_header_id, retries, retry_delay, retry_multiplier):
    print('Invoke Claim Match lambda function')
    payload = {
        "billHeaderId": bill_header_id,
        "tradingPartner":trading_partner,
        "fileHeaderId":file_header_id
    }
    print(f"Payload to Lambda: {json.dumps(payload)}")
    delay = retry_delay
    claim_match_response_status=None
    statusCode=""
    boto_config = Config(connect_timeout=120, read_timeout=300)
    lambda_client = boto3.client('lambda',region_name='us-east-1',config=boto_config)
    
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
    
            if response_payload:
                claim_match_response_status = response_payload
            else:
                claim_match_response_status='FAILED IN LAMBDA'
                raise Exception(f"Lambda response not in expected format. {response_payload}")
            
            # When API call fails and list of error codes retry mechanism (Error Codes NOT DEFINED) - Temporarirly enabled retry if response is not Success or Valid failures
            if 'Inserted record successfully' not in response_payload and 'No records found' not in response_payload and 'Data not received from GW for provided claim' not in response_payload: #statusCode in ['502','503','504','UNKNOWN']
                print(f'Retry delayed {delay} for Attempt {attempt + 1}...')
                time.sleep(delay)
                delay = retry_delay * retry_multiplier
            else:
                break
            
        except Exception as e:
            print(f"Error invoking Lambda function: {str(e)}")
            claim_match_response_status='FAILED IN LAMBDA'
            print(f'Retry delayed {delay} for Attempt {attempt + 1}...')
            time.sleep(delay)
            delay = retry_delay * retry_multiplier
        
    return claim_match_response_status
        
## @params: [claimant-billingestion-enhanced-load-job]
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'env_profile',
    'source',
    'trading_partner',
    'file_header_id',
    'db_secret_key',
    'datacatalog_schema_name',
    'datacatalog_config_table_name',
    'sns_topic_arn',
    's3_bucket',
    's3_error_attachment_prefix',
    'error_alerts_to_email_ids',
    'lambda_function_claim_match',
    'lambda_function_nodemailer',
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
datacatalog_schema_name=args['datacatalog_schema_name']
datacatalog_config_table_name=args['datacatalog_config_table_name']
sns_topic_arn=args['sns_topic_arn']
s3_bucket=args['s3_bucket']
s3_error_attachment_prefix=args['s3_error_attachment_prefix']
error_alerts_to_email_ids = args['error_alerts_to_email_ids']
lambda_function_claim_match=args['lambda_function_claim_match']
lambda_function_nodemailer = args['lambda_function_nodemailer']
retries=int(args['retries'])
retry_delay=int(args['retry_delay'])
retry_multiplier=int(args['retry_multiplier'])
reprocess_flag=args.get('reprocess_flag', 'N').upper()  # Default to N if not present

#Initializations
batch_id=None
is_validation = False
is_transformation = False
is_validation_errors=False
is_transform_errors=False
is_skip_execution = False
is_no_data_execution = False
log_status = ""
mode = "append"

spark.conf.set("spark.sql.ansi.enabled","True")

try:
    logger.info(f"Processing STARTED for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id}...")

    logger.info(f"db_secret_key => {db_secret_key}")
    secrets = get_secret(db_secret_key, 'us-east-1')

    db_sp_params = trading_partner, file_header_id, None, job_id, 'Glue - ETL', 'Glue - ETL', 0
    logger.info(f"db_sp_params => {db_sp_params}")
    dbStoredProcedure = "txm_bitx_enhanced_logs.insert_enhanced_logs"
    logger.info(f"dbStoredProcedure => {dbStoredProcedure}")
    batch_id = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, 6)
    logger.info(f"batch_id => {batch_id}")            
 
    logger.info(f"reprocess_flag => {reprocess_flag}")
    
    # Read logtable records from db with filtered file_header_id and trading_partner.
    read_db_sql = f"""
        SELECT * FROM txm_bitx_enhanced_logs.batch_enhanced_master 
        WHERE trading_partner = '{trading_partner}' 
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
            if log_status.startswith('COMPLETED'):
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
        logger.info(f"No existing entry found for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id}. Proceeding with enhanced load...")

    if not is_skip_execution:
        #Register udf
        def notNull_notEmpty_udf(a,b,c,d,e,f):
            return all(value is not None and value!=''for value in[a,b,c,d,e,f] )
        
        spark.udf.register("notNull_notEmpty_udf",notNull_notEmpty_udf,BooleanType())
        if "notNull_notEmpty_udf" in [f.name for f in spark.catalog.listFunctions()]:
            logger.info("UDF notNull_notEmpty_udf registered successfully")
        else:
            logger.info("UDF notNull_notEmpty_udf registered failed")

        def validate_NABP_udf(nabp):
            if nabp is None or nabp == '' or len(nabp) != 7 or not nabp.isdigit():
                return None      # Invalid NABP length or None input or Not digits
            #Convert Input NABP to list of nabp digits
            nabp_digits_list = [int(digit) for digit in nabp]
            #Extract last_digit
            last_digit = nabp_digits_list[-1]
            #Calculate the odd sum: (digit0 + digit2 + digit4) and the even sum * 2: (digit1 + digit3 + digit5) * 2 
            odd_sum=sum(nabp_digits_list[0:-1:2])
            even_sum=sum(nabp_digits_list[1:-1:2]) * 2
            #Calculate Check Digit with SUM(odd sum, even sum) % 10
            check_digit = (odd_sum + even_sum) % 10
            #Validate NABP last_digit should match check_digit
            if last_digit == check_digit:
                return nabp
            else:
                return None
        
        spark.udf.register("validate_NABP_udf",validate_NABP_udf,StringType())
        if "validate_NABP_udf" in [f.name for f in spark.catalog.listFunctions()]:
            logger.info("Validate NABP UDF registered successfully")
        else:
            logger.info("Validate NABP UDF registered failed")

        def validate_NPI_udf(npi):
            if npi is None or len(npi) not in [9,10]:
                return None      # Invalid NPI length or None input
            #calculate check digit 
            check_digit=calc_check_digit(npi[:9])
            if len(npi)==9:
                return npi + str(check_digit)
            elif len(npi)==10:
                if int(npi[9]) ==check_digit:
                    return npi
                else:
                    return None
            else:
                return None
        spark.udf.register("validate_NPI_udf",validate_NPI_udf,StringType())
        if "validate_NPI_udf" in [f.name for f in spark.catalog.listFunctions()]:
            logger.info("Validate NPI UDF registered successfully")
        else:
            logger.info("Validate NPI UDF registered failed")
        
        def get_provider_name_udf(provider_last_name, provider_first_name, provider_middle_name, provider_degree):
            """This function will generate provider name based on the input."""
            provider_last_name = provider_last_name.replace(',',' ').strip() if provider_last_name else ''
            provider_first_name = provider_first_name.replace(',',' ').strip() if provider_first_name else ''
            provider_middle_name = provider_middle_name.replace(',',' ').strip() if provider_middle_name else ''
            provider_degree = provider_degree.strip() if provider_degree else ''

            if provider_degree:
                provider_last_name = f"{provider_last_name} {provider_degree}"
            
            if provider_middle_name:
                provider_name = f"{provider_last_name}, {provider_first_name} {provider_middle_name}"
            elif not provider_last_name:
                provider_name = f"{provider_first_name}"
            elif not provider_first_name:
                provider_name = f"{provider_last_name}"
            else:
                provider_name = f"{provider_last_name}, {provider_first_name}"

            return provider_name
        spark.udf.register("get_provider_name_udf",get_provider_name_udf,StringType())
        if "get_provider_name_udf" in [f.name for f in spark.catalog.listFunctions()]:
            logger.info("UDF get_provider_name_udf registered successfully")
        else:
            logger.info("UDF get_provider_name_udf registered failed")

        logger.info(f"Reading Bill Ingestion configuration from Data Catalog table => {datacatalog_schema_name}.{datacatalog_config_table_name}...")
        AWSGlueDataCatalog_EnhancedConfig = glueContext.create_dynamic_frame.from_catalog(database=datacatalog_schema_name, table_name=datacatalog_config_table_name, transformation_ctx="AWSGlueDataCatalog_StagingConfig")
        AWSGlueDataCatalog_EnhancedConfig.printSchema()
        AWSGlueDataCatalog_EnhancedConfig.show()
        config_df = AWSGlueDataCatalog_EnhancedConfig.toDF()
        config_rec = config_df.filter(lower(col("trading_partner")) == trading_partner.lower()).first()
        logger.info(f"config_rec => {config_rec}")

        #Validating Staging table data and loaded into a dataframe
        bill_list_df = get_df_db_data_with_query(config_rec["db_read_sql_select"].replace('<INPUT_FILE_HEADER_ID>', file_header_id), secrets)
        bill_list_df = bill_list_df.cache()
        bill_list_df.createOrReplaceTempView(config_rec["df_table_name"])
        bill_list_df.printSchema()
        bill_list_df.show(5, truncate=False)
        bill_list_count = bill_list_df.count()
        logger.info(f"Bill list Record Count => {bill_list_count}")

        # Check if data available for Input Trading Partner and File Header ID
        if bill_list_count > 0:
            logger.info(f"Data Available for trading partner {trading_partner} and File Header ID {file_header_id}...")
        
            file_name=bill_list_df.first()["file_name"]
            logger.info(f"file_name => {file_name}")
            
            #Invoking claim search API to get claim data for guidewire validations for align techhealth optum
            if trading_partner in ["align","optum","techhealth"]:
                logger.info(f"bill_list_df partition count => {bill_list_df.rdd.getNumPartitions()}")

                if reprocess_flag == 'Y':
                    # Table names
                    main_table = "txm_bitx_staging.claim_data"
                    child_table1 = "txm_bitx_staging.claim_condition_data"
                    child_table2 = "txm_bitx_staging.claim_denial_data"

                    # Delete data from child tables
                    clean_claim_data_tables(child_table1, True, main_table, trading_partner, file_header_id, secrets)
                    clean_claim_data_tables(child_table2, True, main_table, trading_partner, file_header_id, secrets)

                    # Delete data from main table
                    clean_claim_data_tables(main_table, False, main_table, trading_partner, file_header_id, secrets)
                else:
                    logger.info(f"New load, no data to delete.")
                    
                #Register udf
                claimMatchUDF = udf(invoke_claim_match,StringType())

                logger.info('Invoking Claim Match Lambda for Validations...')
                claim_match_response_df = bill_list_df.withColumn("claim_match",claimMatchUDF(bill_list_df['bill_header_id'],lit(trading_partner),lit(file_header_id),lit(retries),lit(retry_delay),lit(retry_multiplier))) 
                claim_match_response_df = claim_match_response_df.cache()
                claim_match_response_df.printSchema()
                claim_match_response_df.show(5, truncate=False)
                claim_match_response_count = claim_match_response_df.count()
                logger.info(f"Claim Match Response Record Count => {claim_match_response_count}")

                claim_match_response_df.groupBy('claim_match').count().show(truncate=False)

            logger.info(f'Reading {trading_partner} Validation configuration from Data Catalog table => {config_rec["datacatalog_schema_name"]}.{config_rec["datacatalog_validation_table_name"]}...')
            AWSGlueDataCatalog_ValidationTPConfig = glueContext.create_dynamic_frame.from_catalog(database=config_rec["datacatalog_schema_name"], table_name=config_rec["datacatalog_validation_table_name"], transformation_ctx="AWSGlueDataCatalog_ValidationTPConfig")
            AWSGlueDataCatalog_ValidationTPConfig.printSchema()
            AWSGlueDataCatalog_ValidationTPConfig.show()
            tp_validation_config_df = AWSGlueDataCatalog_ValidationTPConfig.toDF()
            tp_validation_config_df.show(3, truncate=False)
            validation_config_count = tp_validation_config_df.count()
            logger.info(f"Trading Partner Validation config Record Count => {validation_config_count}")
            tp_validation_config_itr = tp_validation_config_df.orderBy(['order_seq_no']).rdd.toLocalIterator()

            validation_result_df = spark.createDataFrame([], StructType([]))
            validation_result_df.printSchema()
            validation_result_df.show(5, truncate=False)
            recordCount = validation_result_df.count()
            logger.info(f"Validation Result Record Count => {recordCount}")

            primary_validation_errors_df = spark.createDataFrame([], StructType([]))
            primary_validation_errors_df.printSchema()
            primary_validation_errors_df.show(5, truncate=False)
            recordCount = primary_validation_errors_df.count()
            logger.info(f"Primary Validation Errors Result Record Count => {recordCount}")

            is_validation = True
            is_transformation = False
            #Processing Trading Partner Validation configuration in order sequence
            for row in tp_validation_config_itr:
                logger.info(f"Processing started for row: {row}")

                #Validating Staging table data and loaded into a dataframe
                validation_df = get_df_db_data_with_query(row["db_validate_sql_select"].replace('<INPUT_FILE_HEADER_ID>', file_header_id), secrets)
                validation_df = validation_df.cache()
                validation_df.createOrReplaceTempView(row["df_table_name"])
                validation_df.printSchema()
                validation_df.show(5, truncate=False)
                recordCount = validation_df.count()
                logger.info(f"Validation result Record Count => {recordCount}")

                join_condition = [bill_list_df['bill_header_id'] == validation_df['bill_header_id']]
                temp_validation_result_df = bill_list_df.join(validation_df, join_condition, 'left')
                temp_validation_result_df = temp_validation_result_df.withColumn("validation_no",lit(row["order_seq_no"])) \
                    .withColumn("validation_type",lit(row["validation_type"])) \
                    .withColumn("validation_error_message",when(col("validation_result")=="FAIL",lit(row["error_desc"])).otherwise(col("validation_result"))) \
                    .select(bill_list_df["file_header_id"],bill_list_df["bill_header_id"],col("field_name_1"),col("field_value_1"),col("field_name_2"),col("field_value_2"),col("unique_bill_id"),col("validation_no"),col("validation_type"),col("validation_result"),col("validation_error_message")) \
                    .cache()
                temp_validation_result_df.printSchema()
                temp_validation_result_df.show(5, truncate=False)
                recordCount = temp_validation_result_df.count()
                logger.info(f"Temp Validation result Record Count => {recordCount}")

                if row["validation_type"] == "primary":
                    if primary_validation_errors_df.count() == 0:
                        logger.info(f"primary_validation_errors_df Record Count is 0")
                        primary_validation_errors_df = temp_validation_result_df.filter(temp_validation_result_df["validation_result"] == "FAIL").cache()
                    else:
                        primary_validation_errors_df = primary_validation_errors_df.union(temp_validation_result_df.filter(temp_validation_result_df["validation_result"] == "FAIL")).cache()
                    primary_validation_errors_df.printSchema()
                    primary_validation_errors_df.show(5, truncate=False)
                    recordCount = primary_validation_errors_df.count()
                    logger.info(f"Primary Validation Error Result Record Count => {recordCount}")
                else:
                    if primary_validation_errors_df.count() == 0:
                        logger.info(f"primary_validation_errors_df Record Count is 0")
                    else:
                        logger.info("Filtering Primary Validation errors...")
                        join_condition = [temp_validation_result_df['bill_header_id'] == primary_validation_errors_df['bill_header_id']]
                        temp_validation_result_df = temp_validation_result_df.join(primary_validation_errors_df, join_condition, 'leftanti')
                        temp_validation_result_df.printSchema()
                        temp_validation_result_df.show(5, truncate=False)
                        recordCount = temp_validation_result_df.count()
                        logger.info(f"Temp Validation result FILTERED Record Count => {recordCount}")

                # Create validation_error_log_df with filter on column validation_result as FAIL on validation_df
                # Add required columns as per error table txm_bitx_enhanced_logs.batch_enhanced_error_details (except error_id)
                v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                logger.info(f"v_load_timestamp => {v_load_timestamp}")
                temp_validation_error_log_df=temp_validation_result_df.filter(temp_validation_result_df["validation_result"] == "FAIL") \
                    .withColumn('batch_id',lit(batch_id)) \
                    .withColumn('error_code',lit('Glue')) \
                    .withColumn('error_type',concat(lit("Validation "),col("validation_no"))) \
                    .withColumnRenamed('validation_error_message','error_message') \
                    .withColumn('create_user',lit('Glue - ETL')) \
                    .withColumn('update_user',lit('Glue - ETL')) \
                    .withColumn('create_timestamp',lit(v_load_timestamp)) \
                    .withColumn('update_timestamp',lit(v_load_timestamp)) \
                    .select(col("batch_id"),col("bill_header_id"),col("error_code"),col("error_type"),col("error_message"),col("create_user"),col("update_user"),col("create_timestamp"),col("update_timestamp"))
                temp_validation_error_log_df.printSchema()
                temp_validation_error_log_df.show(5, truncate=False)
                errorRecordCount = temp_validation_error_log_df.count()
                logger.info(f"Temp Validation Error result Record Count => {errorRecordCount}")

                if errorRecordCount > 0:
                    log_error_table_db="txm_bitx_enhanced_logs.batch_enhanced_error_details"
                    logger.info(f'Load validation_error_log_df dataframe to RDS table => {log_error_table_db}')
                    load_data_to_rds(temp_validation_error_log_df, log_error_table_db, secrets, mode)
                else:
                    logger.info(f'No Validation errors found, Skipping DB error log load.')

                # Append joined_df to validation_result_df
                if validation_result_df.count() == 0:
                    logger.info(f"validation_result_df Record Count is 0")
                    validation_result_df = temp_validation_result_df.cache()
                else:
                    validation_result_df = validation_result_df.union(temp_validation_result_df).cache()
                validation_result_df.printSchema()
                validation_result_df.show(5, truncate=False)
                recordCount = validation_result_df.count()
                logger.info(f"Validation Result Record Count => {recordCount}")
            windowSpec = Window.partitionBy("bill_header_id")
            validation_result_df = validation_result_df.withColumn('validation_bill_count', count(col("validation_no")).over(windowSpec))
            validation_result_df.printSchema()
            validation_result_df.show(5, truncate=False)
            recordCount = validation_result_df.count()
            logger.info(f"Validation Result (Added Validation count) Record Count => {recordCount}")

            # Group all validation error of each bill records and send error mail separately for each bill record use validation_result_df filter FAIL records
            logger.info("Grouping Bill validation errors and Sending mail alerts...")
            validation_error_df = validation_result_df.filter(validation_result_df["validation_result"] == "FAIL") \
                .select(col("bill_header_id"),col("validation_error_message"),col('field_name_1'),col('field_value_1'),col('field_name_2'),col('field_value_2'),col('unique_bill_id')) \
                .cache()
            validation_error_df.printSchema()
            validation_error_df.show(5, truncate=False)
            validation_error_count = validation_error_df.count()
            logger.info(f"Validation Error Bill Record Count => {validation_error_count}")

            if validation_error_count > 0:
                is_validation_errors = True
                logger.info(f"Found Validation Errors. Sending Consolidated Error mails with error details with attach ")
                process='eBill Validations'
                response = log_and_email_attachment(process,env_profile, file_header_id,batch_id, trading_partner, s3_bucket, s3_error_attachment_prefix, s3_error_attachment_prefix,file_name)
                logger.info(f"ErrorLog File Created successfully: {response}")

            else:
                logger.info("No Validation Error found in the Batch. Proceeding for Transform and Load...")

            # If Validation failed for all bills skip enhanced load i.e no records with validation_result as PASS in validation_result_df (Raise custom error)
            validation_pass_bill_df = validation_result_df.filter(validation_result_df["validation_result"] == "PASS")
            windowSpec = Window.partitionBy("bill_header_id")
            validation_pass_bill_df = validation_pass_bill_df.withColumn('validation_bill_pass_count', count(col("validation_no")).over(windowSpec))
            validation_pass_bill_df = validation_pass_bill_df.filter(validation_pass_bill_df["validation_bill_pass_count"] == validation_pass_bill_df["validation_bill_count"])
            validation_pass_bill_df.printSchema()
            validation_pass_bill_df.show(5, truncate=False)
            validation_pass_count = validation_pass_bill_df.count()
            logger.info(f"Validation PASS Result Record Count => {validation_pass_count}")
            if validation_pass_count > 0:
                logger.info("Found Validation PASSed Bills in the Batch. Proceeding for Transform and Load...")
            else:
                #Error Flow
                error_message = f"CustomError: All Bills Validation FAILED in the Batch. Stopping batch processing without Transform and Load!!!"
                logger.error(f"error_message => {error_message}")
                raise Exception(error_message)
            
            #updated log table
            db_sp_params = batch_id, None, None, 'VALIDATION COMPLETED', 'Glue', None, None, 'Glue - ETL', 'Glue - ETL'
            logger.info(f"db_sp_params => {db_sp_params}")
            dbStoredProcedure = "txm_bitx_enhanced_logs.update_enhanced_log_status"
            logger.info(f"dbStoredProcedure => {dbStoredProcedure}")
            res = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, None)
            logger.info(f"res => {res}")

            logger.info(f'Reading {trading_partner} Transformation configuration from Data Catalog table => {config_rec["datacatalog_schema_name"]}.{config_rec["datacatalog_transformation_table_name"]}...')
            AWSGlueDataCatalog_EnhancedTPConfig = glueContext.create_dynamic_frame.from_catalog(database=config_rec["datacatalog_schema_name"], table_name=config_rec["datacatalog_transformation_table_name"], transformation_ctx="AWSGlueDataCatalog_EnhancedTPConfig")
            AWSGlueDataCatalog_EnhancedTPConfig.printSchema()
            AWSGlueDataCatalog_EnhancedTPConfig.show()
            tp_transformation_config_df = AWSGlueDataCatalog_EnhancedTPConfig.toDF()
            tp_transformation_config_df.show(3, truncate=False)
            recordCount = tp_transformation_config_df.count()
            logger.info(f"Trading Partner config Record Count => {recordCount}")
            is_validation = False
            is_transformation = True

            #Checking if this transaction is in failed state from log table
            # If entry exists, check for partial data and delete enhance data and continue
            if reprocess_flag == 'Y':
                sorted_df = tp_transformation_config_df.orderBy('order_seq_no')
                sorted_list = sorted_df.collect()
                #Retrieve the ID values from the first table
                if sorted_list:
                    first_table = sorted_list[0]
                    first_table_name = first_table["db_table_name"]
                    id_column_name = "bill_id"  # get bill_id value from first table
                    get_ids_query = f"""SELECT {id_column_name} FROM {first_table_name} f WHERE f.source = '{source}' AND f.trading_partner = '{trading_partner}' AND f.file_header_id = {file_header_id}"""
                    partial_bill_ids_df = get_df_db_data_with_query(get_ids_query, secrets)
                    partial_bill_ids_df = partial_bill_ids_df.cache()
                    partial_bill_ids_df.printSchema()
                    partial_bill_ids_df.show(3, truncate=False)
                    partial_bill_ids_count = partial_bill_ids_df.count()
                    logger.info(f"Partial Bill IDs Record Count => {partial_bill_ids_count}")
                    if partial_bill_ids_count > 0:
                        logger.info(f"Deletion of Partial data started...")
                        for row in reversed(sorted_list):
                            table_name = row["db_table_name"]
            
                            delete_script = f"""DELETE t FROM {table_name} t JOIN {first_table_name} f ON f.{id_column_name}=t.{id_column_name} WHERE f.source = '{source}' AND f.trading_partner = '{trading_partner}' AND f.file_header_id = {file_header_id}"""
                            logger.info(delete_script)
                            # Delete data from the table
                            run_sql_query(delete_script, secrets)
                            logger.info(f"Successfully deleted partial data => {table_name}")
            else:
                logger.info(f"New load no data to delete.")
                                

            tp_form_type_df = tp_transformation_config_df.select(col("form_type")).distinct()
            tp_form_type_df.printSchema()
            tp_form_type_df.show(10, truncate=False)
            recordCount = tp_form_type_df.count()
            logger.info(f"Trading Partner form type Record Count => {recordCount}")
            tp_form_type_itr = tp_form_type_df.orderBy(['form_type']).rdd.toLocalIterator()
            #Processing Trading Partner Transformation configuration in order sequence
            for frow in tp_form_type_itr:
                logger.info(f'Processing started for Form Type - {frow["form_type"]}. frow: {frow}')
                
                tp_transformation_config_itr = tp_transformation_config_df.filter(tp_transformation_config_df["form_type"] == frow["form_type"]).orderBy(['order_seq_no']).rdd.toLocalIterator()
                
                bill_header_table_name = ""
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
                    enhanced_df = get_df_db_data_with_query(row["db_read_sql_select"].replace('<INPUT_FILE_HEADER_ID>', file_header_id), secrets)
                    enhanced_df = enhanced_df.cache()
                    enhanced_df.printSchema()
                    enhanced_df.show(5, truncate=False)
                    enhancedRecordCount = enhanced_df.count()
                    logger.info(f"Enhanced load DB read result Record Count => {enhancedRecordCount}")

                    if row["table_type"] == "BILL_HEADER" and enhancedRecordCount == 0:
                        logger.info(f'No Bills FOUND for form type {frow["form_type"]}, So skipping further Transform load!!!')
                        break

                    if validation_error_count > 0:
                        #Filter validation passed bill records alone (Use enhanced_df left anti join validation_error_df)
                        logger.info("Filtering Enhanced DF to skip Validation failed Bills...")
                        join_condition = [enhanced_df['bill_header_id'] == validation_error_df['bill_header_id']]
                        enhanced_filtered_df = enhanced_df.join(validation_error_df, join_condition, 'leftanti').cache()
                        enhanced_filtered_df.printSchema()
                        enhanced_filtered_df.show(5, truncate=False)
                        enhancedFilteredRecordCount = enhanced_filtered_df.count()
                        logger.info(f"Enhanced load DB read result (exclude validation errors) Record Count => {enhancedFilteredRecordCount}")

                        if row["table_type"] == "BILL_HEADER" and enhancedFilteredRecordCount == 0:
                            logger.info(f'No Bills FOUND for form type {frow["form_type"]} after filtering Validation errors, So skipping further Transform load!!!')
                            break
                    else:
                        logger.info("No Validation Errors found in the Batch. Skipping Validation failed Bills Filter...")
                        enhanced_filtered_df = enhanced_df

                    # Split Line Flow
                    is_split_line = False
                    is_pc_split_line = False
                    if trading_partner == "jopari" and frow["form_type"] in ["CMS1500","UB04"] and row["table_type"] == "LINE_DETAIL":

                        logger.info(f'Starting split line logic...')

                        charge_column = "charge_for_service" if frow["form_type"] == "UB04" else "charges_for_services"
                        unit_column = "day_unit_qty" if frow["form_type"] == "UB04" else "units" 
                        
                        logger.info(f"charge_col {charge_column}")
                        logger.info(f"unit_col {unit_column}")

                        enhanced_split_line_df = enhanced_filtered_df.filter(enhanced_filtered_df[charge_column] >= 100000).cache()
                        enhanced_split_line_df.printSchema()
                        enhanced_split_line_df.show(5, truncate=False)
                        split_line_record_count = enhanced_split_line_df.count()
                        logger.info(f"Enhanced Split Line Record Count => {split_line_record_count}")

                        if split_line_record_count > 0:
                            logger.info(f'Line records found for split line logic...')

                            is_split_line = True

                            logger.info("Filtering Enhanced DF to skip line records considered for Split Line flow...")
                            join_condition = [enhanced_filtered_df['bill_detail_record_id'] == enhanced_split_line_df['bill_detail_record_id']]
                            final_enhanced_df = enhanced_filtered_df.join(enhanced_split_line_df, join_condition, 'leftanti').cache()
                            final_enhanced_df.printSchema()
                            final_enhanced_df.show(5, truncate=False)
                            recordCount = final_enhanced_df.count()
                            logger.info(f"Enahnced Filtered DF without Split Line rows Record Count => {recordCount}")

                            split_line_df = spark.createDataFrame([], StructType([]))
                            split_line_df.printSchema()
                            split_line_df.show(5, truncate=False)
                            recordCount = split_line_df.count()
                            logger.info(f"Split Line Record Count => {recordCount}")

                            enhanced_split_line_itr = enhanced_split_line_df.rdd.toLocalIterator()
                            #Split Line logic
                            for split_row in enhanced_split_line_itr:
                                new_charges_split_1 = 0
                                new_units_split_1 = 0
                                split_line_count_1 = 0
                                new_charges_split_2 = 0
                                new_units_split_2 = 0
                                split_line_count_2 = 0
                                old_charges = split_row[charge_column]
                                old_units = split_row[unit_column]
                                #logger.info(f"old_charges => {old_charges}")
                                #logger.info(f"old_units => {old_units}")

                                for cnt in range(2,1001):
                                    temp_charges = old_charges / cnt
                                    #logger.info(f"temp_charges => {temp_charges}")
                                    if temp_charges < 100000.00:
                                        if old_units < cnt:
                                            old_units = cnt
                                        
                                        # Spliting charges to avoid round off for new charges. Ex. 277625 by 3 should result in new units set as 92541.67, 92541.67, 92541.66
                                        # Spliting units to avoid decimal values for new units. Ex. 10 by 3 should result in new units set as 3, 3, 4
                                        new_charges_split_1 = builtins.round(old_charges / cnt, 2)
                                        new_units_split_1 = builtins.round(old_units / cnt, 0)
                                        split_line_count_1 = cnt - 1
                                        #logger.info(f"cnt => {cnt}")
                                        #logger.info(f"new_charges_split_1 => {new_charges_split_1}")
                                        #logger.info(f"new_units_split_1 => {new_units_split_1}")
                                        #logger.info(f"split_line_count_1 => {split_line_count_1}")

                                        temp_split_line_1_df = enhanced_split_line_df.filter(enhanced_split_line_df.bill_detail_record_id == split_row["bill_detail_record_id"]) \
                                                                    .withColumn(charge_column, lit(new_charges_split_1)) \
                                                                    .withColumn(unit_column, lit(new_units_split_1)) \
                                                                    .withColumn('split_line_count', lit(split_line_count_1))
                                        # Append to split_line_df
                                        if split_line_df.count() == 0:
                                            logger.info(f"split_line_df Record Count is 0")
                                            split_line_df = temp_split_line_1_df.cache()
                                        else:
                                            split_line_df = split_line_df.union(temp_split_line_1_df).cache()

                                        new_charges_split_2 = old_charges - (new_charges_split_1 * split_line_count_1)
                                        new_units_split_2 = old_units - (new_units_split_1 * split_line_count_1)
                                        split_line_count_2 = 1
                                        #logger.info(f"cnt => {cnt}")
                                        #logger.info(f"new_charges_split_2 => {new_charges_split_2}")
                                        #logger.info(f"new_units_split_2 => {new_units_split_2}")
                                        #logger.info(f"split_line_count_2 => {split_line_count_2}")

                                        temp_split_line_2_df = enhanced_split_line_df.filter(enhanced_split_line_df.bill_detail_record_id == split_row["bill_detail_record_id"]) \
                                                                    .withColumn(charge_column, lit(new_charges_split_2)) \
                                                                    .withColumn(unit_column, lit(new_units_split_2)) \
                                                                    .withColumn('split_line_count', lit(split_line_count_2))
                                        # Append to split_line_df
                                        if split_line_df.count() == 0:
                                            logger.info(f"split_line_df Record Count is 0")
                                            split_line_df = temp_split_line_2_df.cache()
                                        else:
                                            split_line_df = split_line_df.union(temp_split_line_2_df).cache()
                                        
                                        break # Break split line loop as new_charges < 100000.00
                                    
                            split_line_df.printSchema()
                            split_line_df.show(5, truncate=False)
                            recordCount = split_line_df.count()
                            logger.info(f"Split Line Record Count => {recordCount}")

                            final_enhanced_df = final_enhanced_df.withColumn('split_line_count',lit(1))
                            final_enhanced_df = final_enhanced_df.union(split_line_df)
                            final_enhanced_df.printSchema()
                            final_enhanced_df.show(5, truncate=False)
                            recordCount = final_enhanced_df.count()
                            logger.info(f"Final Enhanced DF before EXPLODE Split Line flow Record Count => {recordCount}")
                            final_enhanced_df = final_enhanced_df.withColumn('split_line_count', expr('explode(array_repeat(split_line_count,int(split_line_count)))')).cache()
                            final_enhanced_df.printSchema()
                            final_enhanced_df.show(5, truncate=False)
                            recordCount = final_enhanced_df.count()
                            logger.info(f"Final Enahnced DF after Split Line flow Record Count => {recordCount}")
                        else:
                            logger.info(f'No Line records found for split line logic!!!')

                        logger.info(f'Starting Procedure/Revenue Code Split Line logic...')
                        if is_split_line:
                            logger.info(f'Create Temp VW based on df final_enhanced_df...')
                            final_enhanced_df.createOrReplaceTempView("df_pc_split_check")
                            pc_enhanced_df = final_enhanced_df
                        else:
                            logger.info(f'Create Temp VW based on df enhanced_filtered_df...')
                            enhanced_filtered_df.createOrReplaceTempView("df_pc_split_check")
                            pc_enhanced_df = enhanced_filtered_df

                        if frow["form_type"] == "CMS1500":
                            sql_query = f"""SELECT pc.*
                                            FROM df_pc_split_check pc
                                            JOIN bitx_static_config.line_item_breakdown cl
                                            WHERE cl.record_type = 'P'
                                            AND cl.procedure_code <= pc.procedure_code
                                            AND cl.end_procedure_code >= pc.procedure_code
                                            AND cl.procedure_code_modifier <= IFNULL(pc.procedure_code_modifier_1, '')
                                            AND cl.end_procedure_code_modifier >= IFNULL(pc.procedure_code_modifier_1, '')
                                            AND cl.maximum_quantity >= pc.{unit_column}
                                            AND cl.breakdown_required = 'X'
                            """
                        elif frow["form_type"] == "UB04":
                            sql_query = f"""SELECT pc.*
                                            FROM df_pc_split_check pc
                                            JOIN bitx_static_config.line_item_breakdown cl
                                            WHERE cl.record_type = 'R'
                                            AND cl.procedure_code <= pc.revenue_code
                                            AND cl.end_procedure_code >= pc.revenue_code
                                            AND cl.maximum_quantity >= pc.{unit_column}
                                            AND cl.breakdown_required = 'X'
                            """
                        
                        logger.info(f'Transform SQL - Procedure/Revenue Code Split Line...')
                        logger.info(f'sql_query => {sql_query}')
                        pc_split_line_df = spark.sql(sql_query)
                        pc_split_line_df.printSchema()
                        pc_split_line_df.show(5, truncate=False)
                        pc_split_line_record_count = pc_split_line_df.count()
                        logger.info(f"Procedure/Revenue Code Split Line Record Count => {pc_split_line_record_count}")

                        if pc_split_line_record_count > 0:
                            logger.info(f'Line records found for Procedure/Revenue Code split line logic...')

                            is_pc_split_line = True

                            logger.info("Filtering Enhanced DF to skip line records considered for Split Line flow...")
                            join_condition = [pc_enhanced_df['bill_detail_record_id'] == pc_split_line_df['bill_detail_record_id']]
                            new_final_enhanced_df = pc_enhanced_df.join(pc_split_line_df, join_condition, 'leftanti').cache()
                            new_final_enhanced_df.printSchema()
                            new_final_enhanced_df.show(5, truncate=False)
                            recordCount = new_final_enhanced_df.count()
                            logger.info(f"PC Enahnced DF without Procedure/Revenue Code Split Line rows Record Count => {recordCount}")

                            pc_split_line_df1 =  pc_split_line_df.filter(col(unit_column) > 1).withColumn(charge_column, round(col(charge_column)/col(unit_column),2)) \
                                                                .withColumn('pc_split_line_count', col(unit_column) - lit(1)) \
                                                                .withColumn(unit_column, lit(1))
                            pc_split_line_df1.printSchema()
                            pc_split_line_df1.show(5, truncate=False)
                            recordCount = pc_split_line_df1.count()
                            logger.info(f"PC Split Line 1 Record Count => {recordCount}")

                            pc_split_line_df2 =  pc_split_line_df.filter(col(unit_column) > 1).withColumn(charge_column, (col(charge_column) - (round(col(charge_column)/col(unit_column),2) * (col(unit_column) - lit(1))))) \
                                                                .withColumn('pc_split_line_count', lit(1)) \
                                                                .withColumn(unit_column, lit(1))
                            pc_split_line_df2.printSchema()
                            pc_split_line_df2.show(5, truncate=False)
                            recordCount = pc_split_line_df2.count()
                            logger.info(f"PC Split Line 2 Record Count => {recordCount}")

                            pc_split_line_df3 =  pc_split_line_df.filter(col(unit_column) == 1).withColumn(charge_column, col(charge_column)/col(unit_column)) \
                                                                .withColumn('pc_split_line_count', col(unit_column)) \
                                                                .withColumn(unit_column, lit(1))
                            pc_split_line_df3.printSchema()
                            pc_split_line_df3.show(5, truncate=False)
                            recordCount = pc_split_line_df3.count()
                            logger.info(f"PC Split Line 3 Record Count => {recordCount}..")

                            new_final_enhanced_df = new_final_enhanced_df.withColumn('pc_split_line_count',lit(1))
                            new_final_enhanced_df = new_final_enhanced_df.union(pc_split_line_df1)
                            new_final_enhanced_df = new_final_enhanced_df.union(pc_split_line_df2)
                            new_final_enhanced_df = new_final_enhanced_df.union(pc_split_line_df3)
                            new_final_enhanced_df.printSchema()
                            new_final_enhanced_df.show(5, truncate=False)
                            recordCount = new_final_enhanced_df.count()
                            logger.info(f"Final PC Enahnced DF before EXPLODE Procedure/Revenue Code Split Line flow Record Count => {recordCount}")
                            new_final_enhanced_df = new_final_enhanced_df.withColumn('pc_split_line_count', expr('explode(array_repeat(pc_split_line_count,int(pc_split_line_count)))')).cache()
                            new_final_enhanced_df.printSchema()
                            new_final_enhanced_df.show(5, truncate=False)
                            recordCount = new_final_enhanced_df.count()
                            logger.info(f"New Final Enahnced DF after Procedure/Revenue Code Split Line flow Record Count => {recordCount}")
                        else:
                            logger.info(f'No Line records found for Procedure/Revenue Code split line logic!!!')

                    if is_pc_split_line:
                        logger.info(f'Create Temp VW based on df new_final_enhanced_df...')
                        new_final_enhanced_df.createOrReplaceTempView(row["df_read_table_name"])
                    elif is_split_line:
                        logger.info(f'Create Temp VW based on df final_enhanced_df...')
                        final_enhanced_df.createOrReplaceTempView(row["df_read_table_name"])
                    else:
                        logger.info(f'Create Temp VW based on df enhanced_filtered_df...')
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
                    load_data_to_rds(enhanced_transform_df, row["db_table_name"], secrets, mode)

                    if row["table_type"] == "BILL_HEADER":
                        bill_header_table_name = row["db_table_name"]

                    logger.info(f'db_select_cols_list => {row["db_select_cols_list"]}')
                    logger.info(f'db_filter_id_col_name => {row["db_filter_id_col_name"]}')
                    if row["db_select_cols_list"] or row["db_filter_id_col_name"]:
                        logger.info(f"Extracting generated id(s)...")
                        if row["db_filter_id_col_name"] == "file_header_id":
                            v_filter_id_col_value = file_header_id
                            logger.info(f"v_filter_id_col_value => {v_filter_id_col_value}")
                        else:
                            v_filter_id_col_value = enhanced_transform_df.first()[row["db_filter_id_col_name"]]
                            logger.info(f"v_filter_id_col_value => {v_filter_id_col_value}")
                            if dict(enhanced_transform_df.dtypes)[row["db_filter_id_col_name"]] not in ["int","bigint"]:
                                v_filter_id_col_value = f"'{v_filter_id_col_value}'"
                                logger.info(f"new v_filter_id_col_value => {v_filter_id_col_value}")
                        db_filter_id_df = get_df_db_data_with_filter_id(source, trading_partner, row["db_table_name"], row["table_type"], row["db_select_cols_list"], row["db_filter_id_col_name"], v_filter_id_col_value, bill_header_table_name, secrets)
                        db_filter_id_df = db_filter_id_df.cache()
                        db_filter_id_df.createOrReplaceTempView("db_" + row["df_table_name"])
                        db_filter_id_df.show(5, truncate=False)
                        recordCount = db_filter_id_df.count()
                        logger.info(f"DB Filter IDs Record Count => {recordCount}")
        else:
            is_no_data_execution = True
            logger.info(f"No Data Avaialble for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id}. Skipping the rest of the flow!!!")
    else:
        logger.info(f"Existing entry found for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id} with status as {log_status} for reprocessing flow. No further processing required, skipping rest of the flow!!!")

    #Data for log table
    if is_validation_errors:
        status="COMPLETED WITH VALIDATION ERRORS"
    elif is_skip_execution:
        status="COMPLETED SKIP REPROCESS"
    elif is_no_data_execution:
        status="FAILED NO DATA"
        #Error Flow
        error_message = f"CustomError: Data NOT Available in Staging for trading partner {trading_partner} and File Header ID {file_header_id}."
        logger.error(f"error_message => {error_message}")
        raise Exception(error_message)
    else:
        status="COMPLETED"
    errorInfo="None"
    errormessage="None"
    logger.info(f"Processing COMPLETED for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id}!!!")
    
except Exception as e:
    if is_validation:
        status="FAILED IN VALIDATION"
        errorInfo="Unable to process 1 or more Bill(s)"
    elif is_transformation:
        status="FAILED IN TRANSFORMATION"
        errorInfo="Unable to process 1 or more Bill(s)"
    elif "CustomError: Duplicate Batch" in str(e):
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
        errorInfo="Unable to process 1 or more Bill(s)"
    
    severity="FATAL"

    logger.info(f"Exception Block Error dump: {str(e)}")
    #errormessage=re.search(r"(.+?Exception:.+?)(?=\n|$)",str(e)).group(1)
    errormessage=str(e)[:500]
    response=log_and_email(env_profile,errorInfo,file_header_id,None,trading_partner,errormessage,severity,sns_topic_arn)
    logger.info(f"Email sent successfully!message_id:{response['MessageId']}")
    raise
finally:
    #updated log table
    db_sp_params = batch_id, None, None, status, 'Glue', errorInfo, errormessage, 'Glue - ETL', 'Glue - ETL'
    logger.info(f"db_sp_params => {db_sp_params}")
    dbStoredProcedure = "txm_bitx_enhanced_logs.update_enhanced_log_status"
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