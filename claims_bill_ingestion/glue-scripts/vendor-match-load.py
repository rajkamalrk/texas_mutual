import sys
import logging
import json
import base64
import boto3
import pytz
import time
import re
from botocore.exceptions import ClientError
from botocore.client import Config
from awsglue.job import Job
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
import pymysql
from datetime import datetime, date
from pyspark.sql.functions import col, udf, lit, regexp_replace, substring, to_date, trim, when, length, expr, rpad
from pyspark.sql.types import StructType, StringType, StructField,IntegerType

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("claimant-billingestion-vendor-match-load-job")

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
        ,update_user = 'Glue - ETL - Vendor Match'
        ,update_timestamp = '{v_load_timestamp}'
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
        SELECT s.status_id, '{status_type}', '{status}', '{status}', 'Glue - ETL - Vendor Match', 'Glue - ETL - Vendor Match', '{v_load_timestamp}', '{v_load_timestamp}'
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
     process=f"Process : {source_tp_str} - Vendor Match (Enrichment)"
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

def log_and_email_attachment(error_df, process, env_profile, file_header_id, source, trading_partner, batch_id, bucket_name, file_name, source_prefix, target_prefix):
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

def validate_license(input_bill_data_df, secrets):
    """
    This method handles the entire License validation and database update process for bill records.
    
    This implementation now uses the License Validation API instead of Glue logic.

    :param input_bill_data_df: DataFrame containing the bill data to be license validated
    :param secrets: Dictionary containing secret information (e.g., database credentials)
    :return: DataFrame with validation results
    """
    
    logger.info(f"Starting License Validation using License Validation API...")
    
    # Clean and prepare the input data
    input_bill_data_df = input_bill_data_df.withColumn("rendering_provider_license", trim(col("rendering_provider_license")))
    input_bill_data_df.show(5, truncate=False)
    bill_data_count = input_bill_data_df.count()
    logger.info(f"bill_data_count before validation begins => {bill_data_count}")
    
    # Register UDF for License Validation API
    licenseValidationUDF = udf(invoke_license_validation, StringType())
    
    # Apply license validation using the UDF
    logger.info('Invoking License Validation API for all records...')
    license_validation_result_df = input_bill_data_df.withColumn(
        "validation_response", 
        licenseValidationUDF(
            col("rendering_provider_license"), 
            col("bill_id"), 
            lit(batch_id), 
            col("txm_invoice_number"),
            lit(trading_partner),
            col("form_type"),
            col("rendering_provider_name"),
            lit(retries), 
            lit(retry_delay), 
            lit(retry_multiplier)
        )
    )
    
    # Cache the result right after UDF call to avoid duplicate invocations
    license_validation_result_df = license_validation_result_df.cache()
    license_validation_result_df.printSchema()
    license_validation_result_df.show(5, truncate=False)
    
    # Count validation results
    validation_result_count = license_validation_result_df.count()
    logger.info(f"License Validation API Response Record Count => {validation_result_count}")

    # Group and show validation response
    license_validation_result_df.groupBy('validation_response').count().show(truncate=False)
    
    # Extract validation status for database updates
    license_validation_result_df = license_validation_result_df.withColumn(
        "validation_result",
        when(col("validation_response").startswith("PASS"), "PASS")
        .when(col("validation_response").startswith("FAIL"), "FAIL")
        .when(col("validation_response").startswith("RENDERING FAIL"), "RENDERING FAIL")
        .otherwise("ERROR")
    )

    # Group and show validation results
    license_validation_result_df.groupBy('validation_result').count().show(truncate=False)

    # Define  table names and SQL update queries
    main_table_name = "txm_bitx.bill_header"
    stage_table_name = f"{main_table_name}_{source}_{trading_partner}_{file_header_id}_stage"  # Name for the stage table

    # Define  update queries based on the pass and fail
    v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    update_validation_status_query = f"""
        UPDATE {main_table_name} main
        JOIN {stage_table_name} stage ON main.bill_id = stage.bill_id
        SET 
        main.manual_review_flag = 
            CASE
                WHEN stage.validation_result IN ('FAIL','RENDERING FAIL','ERROR') THEN 'Y'
                WHEN main.manual_review_flag = 'Y' THEN main.manual_review_flag
                ELSE 'N'
            END,
        main.manual_review_reason = 
            CASE 
                WHEN stage.validation_result IN ('FAIL','ERROR') THEN 'License Failed' 
                WHEN stage.validation_result = 'RENDERING FAIL' THEN 'Rendering License Failed' 
                ELSE main.manual_review_reason 
            END,
        main.update_user = 'Glue - ETL - Vendor Match',
        main.update_timestamp = '{v_load_timestamp}'
    """
    
    # Update all records with validation_result in  license_validation_result_df
    logger.info(f"Calling update_db_records function to update {main_table_name} with license validation status in db...")
    update_db_records(license_validation_result_df, secrets, main_table_name, stage_table_name, update_validation_status_query)
    logger.info(f"Sucessfully update records with license validation status  {main_table_name} table in db...")
    
    # Preprocessing for getting input_df for next validation
    logger.info(f"Preprocessing for next validation........")
    license_validation_result_df = license_validation_result_df.withColumn(
        "manual_review_flag",
        when(license_validation_result_df["validation_result"] == "PASS", "N")
        .when(license_validation_result_df["validation_result"] == "FAIL", "Y")
        .when(license_validation_result_df["validation_result"] == "RENDERING FAIL", "Y")
        .when(license_validation_result_df["validation_result"] == "ERROR", "Y")
    )
    license_validation_result_df.printSchema()
    license_validation_result_df.show(5, truncate=False)
    license_validation_result_df_count= license_validation_result_df.count()
    logger.info(f"license_validation_result_count=> {license_validation_result_df_count}")
    
    return license_validation_result_df

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

def validate_npi(npi_input_df, secrets):    
    """
        Perform NPI validation for a given trading partner, update the manual_review_flag and 
        manual_review_reason fields in the database based on validation results.
    """
    npi_input_df.show(5, truncate=False)
    npi_input_df.printSchema()
    npi_input_count=npi_input_df.count()
    logger.info(f"npi_input_count after filter=> {npi_input_count}")

    # if trading_partner = Optum check value of Rendering NPI  and Referring NPI fields from fetched records
    if trading_partner.lower() == 'optum':
        logger.info(f"Applying NPI validation for trading partner: {trading_partner}")
        npi_result_df = npi_input_df.withColumn('rendering_provider_npi', validateNPIudf(npi_input_df['rendering_provider_npi'])) \
                                    .withColumn('referring_provider_npi', validateNPIudf(npi_input_df['referring_provider_npi'])) \
                                    .withColumn('manual_review_flag', when((col('rendering_provider_npi').isNull()) | (col('referring_provider_npi').isNull()), 'Y').otherwise('N'))
        
    # if trading_partner= jopari or techealth or align  check value of Rendering NPI  and Facility NPI fields from fetched records
    else:
        logger.info(f"Applying NPI validation for trading partner: {trading_partner}")
        # if any of Rendering NPI  and Facility NPI fields is null(FAIL) then manual_review_flag update as 'Y' for that record 
        # if both Rendering NPI  and Facility NPI fields not null then (PASS) then manual_review_flag update as 'N' for that record
        npi_result_df = npi_input_df.withColumn('rendering_provider_npi', validateNPIudf(npi_input_df['rendering_provider_npi'])) \
                                    .withColumn('manual_review_flag', when((col('rendering_provider_npi').isNull()), 'Y').otherwise('N'))
        
    npi_result_df.show(5, truncate=False)
    npi_result_df.printSchema()
    npi_record_count=npi_result_df.count()
    logger.info(f"npi_result_df_record_count after npi validation => {npi_record_count}")

    #Update manual_review_flag in bill header table in db  PASS FAIL AS Y N in  manual_review_flag
    main_table_name = "txm_bitx.bill_header"
    stage_table_name = f"{main_table_name}_{source}_{trading_partner}_{file_header_id}_stage"

    # SQL update query for the manual_review_flag based on the NPI validation result.
    v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    update_query = f"""
        UPDATE {main_table_name} main
        JOIN {stage_table_name} stage ON main.bill_id = stage.bill_id
        SET 
            main.manual_review_flag = 
                CASE
                    WHEN stage.manual_review_flag = 'Y' THEN stage.manual_review_flag
                    WHEN main.manual_review_flag = 'Y' THEN main.manual_review_flag
                    ELSE stage.manual_review_flag
                END,
            main.manual_review_reason = 
                CASE
                    WHEN stage.manual_review_flag = 'Y' THEN 'NPI Validation Failed'
                    ELSE main.manual_review_reason
                END,
            main.update_user = 'Glue - ETL - Vendor Match',
            main.update_timestamp = '{v_load_timestamp}'
    """

    # Execute the update query to update the main table in the database.
    logger.info(f"Executing update query to apply NPI validation results to {main_table_name}...")
    update_db_records(npi_result_df, secrets, main_table_name, stage_table_name, update_query)
    logger.info(f"Successfully updated {main_table_name} table with NPI validation results!")

    return npi_result_df

def validate_provider_required_fields(required_field_input_df, secrets):    
    """
        Perform Provider required fields validation for a given trading partner, update the manual_review_flag and 
        manual_review_reason fields in the database based on validation results.
    """
    required_field_input_df.show(5, truncate=False)
    required_field_input_df.printSchema()
    required_field_input_count=required_field_input_df.count()
    logger.info(f"npi_input_count after filter=> {required_field_input_count}")
    
    logger.info(f"Applying Provider required field validation for trading partner: {trading_partner}")
    required_field_result_df = required_field_input_df.withColumn('manual_review_flag', when((col('rendering_provider_name').isNull()) | (col('rendering_provider_npi').isNull()) | (col('facility_name').isNull()) | (col('facility_address_1').isNull()) | (col('facility_city').isNull()) | (col('facility_state').isNull()) | (col('facility_zip_code').isNull()) | (col('vendor_tax_id').isNull()), 'Y').otherwise('N'))
        
    required_field_result_df.show(5, truncate=False)
    required_field_result_df.printSchema()
    required_field_record_count=required_field_result_df.count()
    logger.info(f"required_field_result_df_record_count after Provider required field validation => {required_field_record_count}")

    #Update manual_review_flag in bill header table in db  PASS FAIL AS Y N in  manual_review_flag
    main_table_name = "txm_bitx.bill_header"
    stage_table_name = f"{main_table_name}_{source}_{trading_partner}_{file_header_id}_stage"

    # SQL update query for the manual_review_flag based on the Provider required field validation result.
    v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    update_query = f"""
        UPDATE {main_table_name} main
        JOIN {stage_table_name} stage ON main.bill_id = stage.bill_id
        SET 
            main.manual_review_flag = 
                CASE
                    WHEN stage.manual_review_flag = 'Y' THEN stage.manual_review_flag
                    WHEN main.manual_review_flag = 'Y' THEN main.manual_review_flag
                    ELSE stage.manual_review_flag
                END,
            main.manual_review_reason = 
                CASE
                    WHEN stage.manual_review_flag = 'Y' THEN 'Provider Creation Missing Fields'
                    ELSE main.manual_review_reason
                END,
            main.update_user = 'Glue - ETL - Vendor Match',
            main.update_timestamp = '{v_load_timestamp}'
    """

    # Execute the update query to update the main table in the database.
    logger.info(f"Executing update query to apply Provider required field validation results to {main_table_name}...")
    update_db_records(required_field_result_df, secrets, main_table_name, stage_table_name, update_query)
    logger.info(f"Successfully updated {main_table_name} table with Provider required field validation results!")

    return required_field_result_df

def invoke_vendor_match(bill_id,batch_id,source,trading_partner,txm_invoice_number, retries, retry_delay, retry_multiplier):
    print('Invoke Vendor Match lambda function')
    payload = {
        "billId": bill_id,
        "batchId":batch_id,
        "source":source,
        "tradingPartner":trading_partner
    }
    print(f"Payload to Lambda: {json.dumps(payload)}")
    delay = retry_delay
    vendor_match_response_status=None
    boto_config = Config(connect_timeout=120, read_timeout=300)
    lambda_client = boto3.client('lambda',region_name='us-east-1',config=boto_config)
    
    for attempt in range(0,retries + 1):
        print(f"Attemp: {attempt}")
        try:
            response = lambda_client.invoke(
                FunctionName=vendor_match_lambda_function,
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
                    vendor_match_response_status='SUCCESS'
                elif lambdaStatus=='error' and 'Error occurred while invoking GW API auto vendor match'.lower() in lambdaErrorMessage.lower():
                    vendor_match_response_status='FAILED IN AUTO VENDOR MATCH'
                elif lambdaStatus=='error' and 'Error occurred while invoking ECI API manual vendor match'.lower() in lambdaErrorMessage.lower():
                    vendor_match_response_status='FAILED IN MANUAL VENDOR MATCH'
                else:
                    if lambdaErrorMessage:
                        vendor_match_response_status = lambdaErrorMessage
                    else:
                        vendor_match_response_status='FAILED IN LAMBDA'
                        raise Exception(f"Lambda response Body not in expected format. {response_payload}")
            else:
                vendor_match_response_status='FAILED IN LAMBDA'
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
            vendor_match_response_status='FAILED IN LAMBDA'
            errorInfo=f"Error occured while invoking vendor match lambda function (Attempt - {attempt})"
            errorMessage=str(e).replace("'","''").replace("\\","\\\\")
            print(f"errorInfo => {errorInfo}")  
            print(f"Error: {errorMessage}")
            v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

            # Capture error details in error log table
            # Add required columns as per error table txm_bitx_enrichment_logs.batch_vendor_match_error_details (except error_id)
            error_insert_sql = f"""INSERT INTO txm_bitx_enrichment_logs.batch_vendor_match_error_details (batch_id,bill_id,txm_invoice_number,source,target,match_type,error_code,error_type,error_message,create_user,update_user,create_timestamp,update_timestamp)
                              VALUES ({batch_id},{bill_id},{txm_invoice_number},'BITX','CLAIM_CENTER','VENDOR_MATCH','Invoke Lambda','{errorInfo}','{errorMessage}','Glue - ETL - Vendor Match Load','Glue - ETL - Vendor Match Load','{v_load_timestamp}','{v_load_timestamp}')"""
            run_sql_query(error_insert_sql,secrets)        
            print(f'Loaded error details into RDS table txm_bitx_enrichment_logs.batch_vendor_match_error_details.')
            print(f'Retry delayed {delay} for Attempt {attempt + 1}...')
            time.sleep(delay)
            delay = retry_delay * retry_multiplier
        
    return vendor_match_response_status

def invoke_license_validation(license_number, bill_id, batch_id, txm_invoice_number, trading_partner, form_type, rendering_provider_name, retries, retry_delay, retry_multiplier):
    """
    Invoke License Validation Lambda function to validate provider license numbers.
    
    This method follows the same UDF pattern as invoke_vendor_match to call the 
    License Validation API instead of implementing complex validation logic in Glue.
    
    :param license_number: Provider license number to validate
    :param bill_id: Bill ID for tracking
    :param batch_id: Batch ID for tracking
    :param txm_invoice_number: TXM invoice number for tracking
    :param trading_partner: Trading Partner for validation
    :param form_type: Form Type for validation
    :param rendering_provider_name: Rendering Provider Name for custom validation
    :param retries: Number of retry attempts
    :param retry_delay: Initial delay between retries in seconds
    :param retry_multiplier: Multiplier for exponential backoff
    :return: Validation result string (PASS/FAIL with reason)
    """
    print('Invoke License Validation lambda function')
    
    # Skip validation for null/empty license numbers
    if not license_number or license_number == "":
        return "PASS - License number is null or empty"
    
    license_number_prefix = license_number[:2]
    license_number_suffix = license_number[-2:]

    # Bypass validations for other state licenses
    if re.match('^[A-Z]*$', license_number_suffix) and license_number_suffix != "TX":
        if license_number[:2] == 'PT':
            return "PASS - Bypass validation for Prefix PT"
        else:
            return "FAIL - Unknown other state license format"
    
    # Skip API call for custom Glue logic
    if trading_partner == "jopari" and form_type == "PROF":
        if license_number == "MDTX" and not re.match('^.*,.*$', rendering_provider_name):
            return "RENDERING FAIL - Custom rule for MDTX"
        if license_number_prefix in ["NP", "RN"]:
            return "RENDERING FAIL - Custom rule for prefix NP and RN"
    
    # Prepare payload for License Validation API
    payload = {
        "license_number": license_number
    }
    print(f"Payload to License Validation Lambda: {json.dumps(payload)}")
    
    delay = retry_delay
    license_validation_response_status = None
    boto_config = Config(connect_timeout=120, read_timeout=300)
    lambda_client = boto3.client('lambda', region_name='us-east-1', config=boto_config)
    
    # Use the license validation lambda function name from job parameters
    
    for attempt in range(0, retries + 1):
        print(f"License Validation Attempt: {attempt}")
        try:
            response = lambda_client.invoke(
                FunctionName=license_validation_lambda_function,
                InvocationType='RequestResponse',
                Payload=json.dumps(payload)
            )
            response_payload = response['Payload'].read().decode('utf-8')
            print(f"License Validation Lambda response payload: {response_payload}")
            response_payload = json.loads(response_payload)
            
            # Handle Lambda response
            if response_payload and 'validation_result' in response_payload:                
                # Extract validation result from the License Validation API response
                validation_result = response_payload.get('validation_result', 'ERROR')
                success_reason = response_payload.get('success_reason', '')
                failure_reason = response_payload.get('failure_reason', '')
                
                print(f"License Validation Result for bill_id {bill_id}: {validation_result}, Success: {success_reason}, Failure: {failure_reason}")
                
                if validation_result == 'PASS':
                    license_validation_response_status = f"PASS - {success_reason}" if success_reason else "PASS"
                elif validation_result == 'FAIL':
                    license_validation_response_status = f"FAIL - {failure_reason}" if failure_reason else "FAIL"
                else:
                    license_validation_response_status = f"ERROR - {validation_result}"
                    raise Exception(f"Unexpected validation result: {validation_result}")
                
                # Break out of retry loop on successful response
                break
                
            else:
                license_validation_response_status = 'ERROR - Invalid Lambda response format'
                raise Exception(f"License Validation Lambda response not in expected format. {response_payload}")
            
        except Exception as e:
            print(f"Error invoking License Validation Lambda function: {str(e)}")
            license_validation_response_status = 'ERROR - Lambda invocation error'
            errorInfo = f"Error occurred while invoking license validation lambda function (Attempt - {attempt})"
            errorMessage = str(e).replace("'", "''").replace("\\", "\\\\")
            print(f"errorInfo => {errorInfo}")
            print(f"Error: {errorMessage}")
            
            # Log error details for debugging
            v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            # Capture error details in error log table
            # Add required columns as per error table txm_bitx_enrichment_logs.batch_vendor_match_error_details (except error_id)
            error_insert_sql = f"""INSERT INTO txm_bitx_enrichment_logs.batch_vendor_match_error_details (batch_id,bill_id,txm_invoice_number,source,target,match_type,error_code,error_type,error_message,create_user,update_user,create_timestamp,update_timestamp)
                              VALUES ({batch_id},{bill_id},{txm_invoice_number},'BITX','LICENSE_VALIDATION_API','LICENSE_VALIDATION','Invoke Lambda','{errorInfo}','{errorMessage}','Glue - ETL - Vendor Match Load','Glue - ETL - Vendor Match Load','{v_load_timestamp}','{v_load_timestamp}')"""
            run_sql_query(error_insert_sql,secrets)        
            print(f'Loaded error details into RDS table txm_bitx_enrichment_logs.batch_vendor_match_error_details.')
            
            # On last attempt, return error status
            if attempt == retries:
                return f"ERROR - License validation error after {retries + 1} attempts: {errorMessage[:100]}"
            
            # Retry with exponential backoff
            print(f'Retry delayed {delay} for Attempt {attempt + 1}...')
            time.sleep(delay)
            delay = retry_delay * retry_multiplier
    
    return license_validation_response_status

## @params: [claimant-billingestion-vendor-match-load-job]
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'env_profile',
    'source',
    'trading_partner',
    'file_header_id',
    'db_secret_key',
    'sns_topic_arn',
    's3_bucket',
    'vendor_match_lambda_function',
    'license_validation_lambda_function',
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
vendor_match_lambda_function=args['vendor_match_lambda_function']
license_validation_lambda_function=args['license_validation_lambda_function']
lambda_function_nodemailer = args['lambda_function_nodemailer']
s3_error_attachment_prefix=args['s3_error_attachment_prefix']
error_alerts_to_email_ids=args['error_alerts_to_email_ids']
retries=int(args['retries'])
retry_delay=int(args['retry_delay'])
retry_multiplier=int(args['retry_multiplier'])
reprocess_flag=args.get('reprocess_flag', 'N').upper()  # Default to N if not present

#Initializations
batch_id=None
is_vendor_match = False
is_vendor_match_error = False
is_skip_vendor_match = False
is_skip_execution = False
is_skip_no_data_error = False
is_no_data_execution = False
log_status = ""
status="COMPLETED"
mode="append"

try:
    logger.info(f"Processing STARTED for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id} ...")

    logger.info(f"db_secret_key => {db_secret_key}")
    secrets = get_secret(db_secret_key, 'us-east-1')

    db_sp_params = source,trading_partner, file_header_id, None, job_id, 'Glue - ETL', 'Glue - ETL', 0
    logger.info(f"db_sp_params => {db_sp_params}")
    dbStoredProcedure = "txm_bitx_enrichment_logs.insert_vendor_match_logs"
    logger.info(f"dbStoredProcedure => {dbStoredProcedure}")
    batch_id = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, 7)
    logger.info(f"batch_id => {batch_id}")

    logger.info(f"reprocess_flag => {reprocess_flag}")
    
    read_db_sql = f"""
        SELECT * FROM txm_bitx_enrichment_logs.batch_vendor_match_master 
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
        logger.info(f"No existing entry found for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id}. Proceeding with claim match...")
    
    if not is_skip_execution:
        if reprocess_flag == 'Y':
            status_where_condition = """AND (   (i.status_type = 'BILL_IMAGE_CREATION' AND i.status = 'COMPLETED')
                                             OR (i.status_type = 'STORAGE_REQUEST' AND i.status IN ('COMPLETED','NOT_APPLICABLE'))
                                             OR (i.status_type IN ('VENDOR_MATCH_AUTO', 'VENDOR_MATCH_MANUAL') AND i.status = 'FAILED') 
                                             OR (i.status_type IN ('VENDOR_MATCH_AUTO') AND i.status = 'IN_PROGRESS' AND i.update_timestamp < NOW() - INTERVAL 1 DAY))"""
        else:
            status_where_condition = """AND (   (i.status_type = 'BILL_IMAGE_CREATION' AND i.status = 'COMPLETED')
                                             OR (i.status_type = 'STORAGE_REQUEST' AND i.status IN ('COMPLETED','NOT_APPLICABLE'))
                                            )"""
        if source == 'kofax':
            read_db_sql = f"""SELECT DISTINCT b.bill_id
                                ,b.txm_invoice_number
                                ,b.source
                                ,b.trading_partner
                                ,b.form_type
                                ,b.manual_review_flag 
                                ,i.status_type 
                                ,i.status
                                ,p.rendering_provider_name
                                ,p.rendering_provider_license
                                ,p.rendering_provider_npi
                                ,p.facility_name
                                ,p.facility_address_1
                                ,p.facility_city
                                ,p.facility_state
                                ,p.facility_zip_code
                                ,CASE WHEN pp.provider_number IS NOT NULL THEN 'Y' ELSE 'N' END AS is_provider_exist
                                ,p.referring_provider_npi
                                ,p.vendor_tax_id
                        FROM txm_bitx.bill_header b
                        JOIN txm_bitx.invoice_status i ON b.txm_invoice_number=i.txm_invoice_number
                        LEFT JOIN txm_bitx.provider p ON b.bill_id = p.bill_id 
                        LEFT JOIN txm_bitx_provider.provider pp on p.provider_number = pp.provider_number  
                        WHERE b.source = '{source}'
                        AND (b.trading_partner IS NULL OR b.trading_partner = '{trading_partner}')
                        {status_where_condition}       
                    """
        else:
            read_db_sql = f"""SELECT DISTINCT b.bill_id
                                    ,b.txm_invoice_number
                                    ,b.source
                                    ,b.trading_partner
                                    ,b.form_type
                                    ,b.manual_review_flag 
                                    ,i.status_type 
                                    ,i.status
                                    ,p.rendering_provider_name
                                    ,p.rendering_provider_license
                                    ,p.rendering_provider_npi
                                    ,p.facility_name
                                    ,p.facility_address_1
                                    ,p.facility_city
                                    ,p.facility_state
                                    ,p.facility_zip_code
                                    ,'N' AS is_provider_exist
                                    ,p.referring_provider_npi
                                    ,p.vendor_tax_id
                            FROM txm_bitx.bill_header b
                            JOIN txm_bitx.invoice_status i ON b.txm_invoice_number=i.txm_invoice_number
                            LEFT JOIN txm_bitx.provider p ON b.bill_id = p.bill_id   
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
            if source.lower() in ('eci','kofax'):
                # Filtering and vendor match skip for Kofax only
                if source.lower() == 'kofax': 
                    bill_data_non_ebill_df = bill_data_df.filter(col('is_provider_exist') == 'Y')
                    bill_data_non_ebill_df.printSchema()
                    bill_data_non_ebill_df.show(5, truncate=False)
                    provider_exists_count = bill_data_non_ebill_df.count()
                else:
                    bill_data_non_ebill_df = bill_data_df

                if source.lower() == 'eci' or (source.lower() == 'kofax' and provider_exists_count > 0 ): # vendor skip logic
                    is_skip_vendor_match = True
                    logger.info(f"Skip the vendor match flow for batch_id => {batch_id} and source type => {source}")

                    logger.info(f"Update invoice_status table...")
                    status_type = 'VENDOR_MATCH_AUTO'
                    status = 'NOT_APPLICABLE'
                    update_invoice_status(bill_data_non_ebill_df, secrets, status_type, status)

                    #updated log table
                    db_sp_params = batch_id, None, None, None, 'VENDOR MATCH SKIP FLOW COMPLETED', 'Glue', None, None, 'Glue - ETL', 'Glue - ETL'
                    logger.info(f"db_sp_params => {db_sp_params}")
                    dbStoredProcedure = "txm_bitx_enrichment_logs.update_vendor_match_log_status"
                    logger.info(f"dbStoredProcedure => {dbStoredProcedure}")
                    res = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, None)
                    logger.info(f"res => {res}")
                else: 
                    logger.info(f"Vendor match flow will proceed for batch_id {batch_id} and source {source} (provider does not exist).")

            if source.lower != 'eci':
                is_vendor_match = True
                logger.info(f"Vendor Match Flow STARTED for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id}...")
                
                if source.lower() == 'kofax':
                    bill_data_df = bill_data_df.filter(col('is_provider_exist') == 'N')
                    bill_data_df.printSchema()
                    bill_data_df.show(5, truncate=False)
                    recordCount = bill_data_df.count()
                    logger.info(f"Non exist provider Record Count => {recordCount}")
                    
                #dropping temporary col
                bill_data_df = bill_data_df.drop("is_provider_exist")

                #Register udf
                validateNPIudf = udf(validate_NPI_udf, StringType())
                
                # Skip validations for Non Ebill, perform it for only EBill
                if source.lower() == 'ebill':
                    # Filtering records manual_review_flag not equal to 'Y' - Override any Address Validation FAILED as it has been made Optional with a configurable switch.
                    input_validation_df=bill_data_df.select("bill_id","txm_invoice_number","form_type","rendering_provider_name","rendering_provider_license","rendering_provider_npi","facility_name","facility_address_1","facility_city","facility_state","facility_zip_code","referring_provider_npi","vendor_tax_id","manual_review_flag")
                    input_validation_df = input_validation_df.cache()
                    input_validation_df.printSchema()
                    input_validation_df.show(5, truncate=False)
                    input_validation_df_count = input_validation_df.count()
                    logger.info(f"input_validation_df Record Count => {input_validation_df_count}")
                    if input_validation_df_count > 0:
                        # NPI Validation 
                        logger.info(f"NPI Validation STARTED for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id}!!!")
                        npi_validated_df=validate_npi(input_validation_df,secrets)
                        npi_validated_df.show(5, truncate=False)   #Ouput dataframe of required field validation process
                        npi_validated_df.printSchema()
                        npi_validated_count=npi_validated_df.count()
                        logger.info(f"npi_validated_count => {npi_validated_count}")
                        logger.info(f"NPI Validation COMPLETED for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id}!!!")

                        # License Validation
                        logger.info(f"License Validation STARTED for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id}...")
                        # Filtering records manual_review_flag not equal to 'Y' - Override any Address Validation FAILED as it has been made Optional with a configurable switch.
                        license_validated_df=validate_license(npi_validated_df.filter((col('manual_review_flag').isNull()) | (col('manual_review_flag') != 'Y')).drop("manual_review_flag"), secrets)
                        license_validated_df.show(5, truncate=False)   #Ouput dataframe of license validation process
                        license_validated_df.printSchema()
                        license_validated_count=license_validated_df.count()
                        logger.info(f"license_validated_count => {license_validated_count}")
                        logger.info(f"License Validation COMPLETED for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id}!!!")
                        
                        # Provider Required fields Validation
                        logger.info(f"Provider Required fields Validation STARTED for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id}...")
                        # Filtering records manual_review_flag not equal to 'Y' - Override any Address Validation FAILED as it has been made Optional with a configurable switch.
                        provider_field_validated_df=validate_provider_required_fields(license_validated_df.filter((col('manual_review_flag').isNull()) | (col('manual_review_flag') != 'Y')),secrets)
                        provider_field_validated_df.show(5, truncate=False)   #Ouput dataframe of required field validation process
                        provider_field_validated_df.printSchema()
                        provider_required_field_validated_count=provider_field_validated_df.count()
                        logger.info(f"provider_required_field_validated_count => {provider_required_field_validated_count}")
                        logger.info(f"Provider Required fields Validation COMPLETED for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id}!!!")
                    else:
                        logger.info(f"Skipping Validations for Source {source}, as NO data avaialble with manual_review_flag != Y...")
                else:
                    logger.info(f"Skipping Validations for Source {source}, as it is Non Ebill...")
                

                # bill_data_df to continue next flow irrespective of validation pass fail                
                logger.info(f"Vendor Match Invoke Lambda STARTED for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id}...")
                
                #Register UDF   
                vendorMatchUDF = udf(invoke_vendor_match,StringType())

                bill_data_df = bill_data_df.drop("rendering_provider_license").distinct()
                logger.info('Invoking Vendor Match Lambda...')    
                vendor_match_response_df= bill_data_df.withColumn("vendor_match",vendorMatchUDF(bill_data_df['bill_id'],lit(batch_id),lit(source),lit(trading_partner),bill_data_df['txm_invoice_number'],lit(retries),lit(retry_delay),lit(retry_multiplier)))
                vendor_match_response_df = vendor_match_response_df.cache()
                vendor_match_response_df.printSchema()
                vendor_match_response_df.show(5, truncate=False)
                vendor_match_response_count = vendor_match_response_df.count()
                logger.info(f"Vendor Match Response Record Count => {vendor_match_response_count}")

                vendor_match_response_df.groupBy('vendor_match').count().show(truncate=False)
                vendor_match_lambda_error_txm_id_df=vendor_match_response_df.filter(vendor_match_response_df["vendor_match"] == "FAILED IN LAMBDA").select("txm_invoice_number").distinct()
                vendor_match_lambda_error_txm_id_df.printSchema()
                vendor_match_lambda_error_txm_id_df.show(5, truncate=False)
                lambdaErrorRecordCount = vendor_match_lambda_error_txm_id_df.count()
                logger.info(f"vendor match Lambda Errors txm_invoice_number Record Count => {lambdaErrorRecordCount}")
                if lambdaErrorRecordCount > 0:
                    logger.info(f"Found Vendor match Lambda Errors. Updating Invoice Status Table as FAILED!!!")
                    logger.info(f"Update invoice_status table...")
                    status_type = 'VENDOR_MATCH_AUTO'
                    status = 'FAILED'
                    update_invoice_status(vendor_match_lambda_error_txm_id_df, secrets, status_type, status)
                else:
                    logger.info(f"No vendor match Lambda Errors Found!!!")
                    
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
                                        ,'Vendor Tax ID' AS field_name_1
                                        ,p.vendor_tax_id AS field_value_1
                                        ,'' AS field_name_2
                                        ,'' AS field_value_2
                                        ,DATE_FORMAT(CONVERT_TZ(IFNULL(e.update_timestamp, e.create_timestamp), 'UTC', 'America/Chicago'), '%Y/%m/%d %H:%i:%sCST') AS update_timestamp
                                        ,s.status_type
                                        ,s.status
                                    FROM txm_bitx.bill_header b
                                    JOIN txm_bitx.invoice_status s ON b.txm_invoice_number = s.txm_invoice_number
                                    JOIN txm_bitx_enrichment_logs.batch_vendor_match_error_details e ON b.bill_id = e.bill_id
                                    LEFT JOIN txm_bitx.provider p ON b.bill_id = p.bill_id
                                    WHERE b.source = '{source}'
                                    AND (b.trading_partner IS NULL OR b.trading_partner = '{trading_partner}')
                                    AND e.batch_id={batch_id}
                            """              

                vendor_match_error_log_df = get_df_db_data_with_query(read_db_sql, secrets)
                vendor_match_error_log_df = vendor_match_error_log_df.cache()
                vendor_match_error_log_df.printSchema()
                vendor_match_error_log_df.show(5, truncate=False)
                vendor_match_error_count = vendor_match_error_log_df.count()
                logger.info(f"vendor match Error Log Record Count => {vendor_match_error_count}")

                if vendor_match_error_count > 0:
                    is_vendor_match_error = True
                    logger.info(f"Found vendor match Errors. Sending Consolidated Error mails with error details as attachment!!!")
                    process='Vendor Match'
                    response = log_and_email_attachment(vendor_match_error_log_df, process, env_profile, file_header_id, source, trading_partner, batch_id, s3_bucket, file_name, s3_error_attachment_prefix, s3_error_attachment_prefix)
                    logger.info(f"ErrorLog File Created successfully: {response}")

                    vendor_match_error_log_txm_id_df=vendor_match_error_log_df \
                        .filter(((col("status_type") == "VENDOR_MATCH_AUTO") & (col("status") == "FAILED")) | ((col("status_type") == "VENDOR_MATCH_MANUAL") & (col("status") == "FAILED"))) \
                        .select("txm_invoice_number").distinct()
                    vendor_match_error_log_txm_id_df.printSchema()
                    vendor_match_error_log_txm_id_df.show(5, truncate=False)
                    error_record_count = vendor_match_error_log_txm_id_df.count()
                    logger.info(f"Vendor Match Errors txm_invoice_number Record Count => {error_record_count}")

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
                    logger.info("No vendor match Errors found in the Batch!!!")

                #updated log table
                db_sp_params = batch_id, None,None,None, 'VENDOR MATCH FLOW COMPLETED', 'Glue', None, None, 'Glue - ETL', 'Glue - ETL'
                logger.info(f"db_sp_params => {db_sp_params}")
                dbStoredProcedure = "txm_bitx_enrichment_logs.update_vendor_match_log_status"
                logger.info(f"dbStoredProcedure => {dbStoredProcedure}")
                res = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, None)
                logger.info(f"res => {res}")
        else:
            is_no_data_execution = True
            logger.info(f"No Data Avaialble for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id}. Skipping the rest of the flow!!!")
    else:
        logger.info(f"Existing entry found for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id} with status as {log_status} for reprocessing flow. No further processing required, skipping rest of the flow!!!")
    
    #Data for log table
    if is_vendor_match_error:
        status="COMPLETED WITH VENDOR MATCH ERRORS"
    elif is_skip_execution:
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
    # log infomation 
except Exception as e:
    if is_vendor_match:
        status="FAILED IN VENDOR MATCH"
        errorInfo="Error occured during Vendor Match Flow"
    elif is_skip_vendor_match:
        status="FAILED IN SKIP VENDOR MATCH"
        errorInfo="Error occured during Skip Vendor Match Flow"
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
    response=log_and_email(env_profile,errorInfo,file_header_id,source,trading_partner,None,errormessage,severity,sns_topic_arn)
    logger.info(f"Email sent successfully!message_id:{response['MessageId']}")
    raise
finally:
    #updated log table
    db_sp_params = batch_id, None, None,None, status, 'Glue', errorInfo, errormessage, 'Glue - ETL', 'Glue - ETL'
    logger.info(f"db_sp_params => {db_sp_params}")
    dbStoredProcedure = "txm_bitx_enrichment_logs.update_vendor_match_log_status"
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