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
from botocore.exceptions import ClientError
from pyspark.context import SparkContext
from pyspark.sql import SQLContext, DataFrame
import pymysql
from datetime import datetime
from botocore.exceptions import ClientError
from pyspark.sql.functions import lit, col
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("preauth_glue_common")

# PreAuth log table variables
PREAUTH_LOG_SCHEMA = "txm_preauth_logs"
BATCH_PREAUTH_MASTER_TABLE = f"{PREAUTH_LOG_SCHEMA}.batch_preauth_master"
BATCH_PREAUTH_STEP_DETAILS_TABLE = f"{PREAUTH_LOG_SCHEMA}.batch_preauth_step_details"
BATCH_PREAUTH_ERROR_DETAILS_TABLE = f"{PREAUTH_LOG_SCHEMA}.batch_preauth_error_details"

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

def move_s3_file(destination_prefix, bucket_name, source_prefix, s3_client=None):
    """
    Move a file from source to destination in S3.
    
    Args:
        destination_prefix (str): Destination S3 key
        bucket_name (str): S3 bucket name
        source_prefix (str): Source S3 key
        s3_client: Optional S3 client, will create one if not provided
        
    Returns:
        str: Success message
    """
    if s3_client is None:
        s3_client = boto3.client('s3')
    
    try:
        # Copy file to destination folder
        s3_client.copy_object(Bucket=bucket_name, Key=destination_prefix, CopySource={'Bucket': bucket_name, 'Key': source_prefix})
        logger.info("File copied successfully.")

        # Delete the original file
        s3_client.delete_object(Bucket=bucket_name, Key=source_prefix)
        logger.info("Original file deleted successfully.")
        return "File moved successfully"

    except Exception as e:
        logger.error(f"Error moving file: {e}")
        raise    

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

def get_df_db_data_with_query(spark,read_db_sql, secrets):
    try:
        df_num_partitions=100
        jdbc_url = f'jdbc:mysql://{secrets["host"]}:{secrets["port"]}/{secrets["dbClusterIdentifier"]}?sessionVariables=group_concat_max_len=1000000&zeroDateTimeBehavior=CONVERT_TO_NULL'
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
    
def get_df_db_data_with_filter_id(spark,table_name, select_cols_list, filter_id_col_name, filter_id_col_value, load_timestamp, secrets):
    """
    Get DataFrame from database with filter conditions.
    
    Args:
        spark: Spark session
        table_name (str): Database table name
        select_cols_list (str): Comma-separated list of columns to select
        filter_id_col_name (str): Column name for filtering
        filter_id_col_value: Value for filtering
        load_timestamp (str, optional): Additional timestamp filter
        secrets (dict): Database connection parameters
        
    Returns:
        DataFrame: Filtered data from database
    """
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
        raise e

def run_insert_command(secrets, insert_sql, insert_values):
    """
    Execute an INSERT command against the database.
    
    Args:
        secrets (dict): Database connection parameters from Secrets Manager
        insert_sql (str): SQL INSERT statement
        insert_values (tuple or list): Single tuple for one row or list of tuples for multiple rows
        
    Returns:
        int: Auto-increment ID of the last inserted row
        
    Raises:
        ValueError: If SQL query is not a valid INSERT statement
        Exception: If database operation fails
    """
    # Validate SQL query
    if not insert_sql.strip().upper().startswith("INSERT"):
        raise ValueError("Invalid INSERT query!!!")
    
    # Establish database connection
    connection = pymysql.connect( 
        host = secrets["host"],
        database = secrets["dbClusterIdentifier"],
        port = secrets["port"],
        user = secrets["username"],
        password = secrets["password"],
        connect_timeout = 300
    )

    auto_increment_id = None
    
    try:
        with connection.cursor() as cursor:
            # Handle both single tuple and list of tuples dynamically
            if isinstance(insert_values, tuple):
                # Single row insert
                cursor.execute(insert_sql, insert_values)
                auto_increment_id = cursor.lastrowid
                logger.info(f"Number of rows inserted: 1")
            elif isinstance(insert_values, list) and len(insert_values) > 0:
                # Multiple rows insert
                cursor.executemany(insert_sql, insert_values)
                auto_increment_id = cursor.lastrowid
                logger.info(f"Number of rows inserted: {cursor.rowcount}")
            else:
                raise ValueError("insert_values must be a tuple or non-empty list of tuples")

            # Commit the transaction
            connection.commit()
    except Exception as e:
        logger.info(f"Insertion Failed in DB: {str(e)}")
        raise e
    finally:
        connection.close()
    return auto_increment_id

def run_update_command(secrets, update_sql, update_values):
    """
    Execute an UPDATE command against the database.
    
    Args:
        secrets (dict): Database connection parameters from Secrets Manager
        update_sql (str): SQL UPDATE statement
        update_values (tuple): Values to bind to the UPDATE statement
        
    Raises:
        ValueError: If SQL query is not a valid UPDATE statement
        Exception: If database operation fails
    """
    # Validate SQL query
    if not update_sql.strip().upper().startswith("UPDATE"):
        raise ValueError("Invalid UPDATE statement!!!")
    
    # Establish database connection
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
            # Execute the update query
            cursor.execute(update_sql, update_values)

            # Log the number of rows updated
            logger.info(f"Number of rows updated: {cursor.rowcount}")

            # Commit the transaction
            connection.commit()
    except Exception as e:
        logger.info(f"Update Failed in DB: {str(e)}")
        raise e
    finally:
        connection.close()     

def run_sql_query(db_sql, secrets):
    """
    Execute a SQL query against the database.
    
    Args:
        secrets (dict): Database connection parameters from Secrets Manager
        db_sql (str): SQL query to execute
        
    Returns:
        list: Query results for SELECT queries, None for other queries
    """
    connection = pymysql.connect( 
        host = secrets["host"],
        database = secrets["dbClusterIdentifier"],
        port = secrets["port"],
        user = secrets["username"],
        password = secrets["password"],
        connect_timeout = 300,
        cursorclass=pymysql.cursors.DictCursor
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute(db_sql)
            if db_sql.strip().upper().startswith("SELECT") or db_sql.strip().upper().startswith("WITH"):
                output = cursor.fetchall()  # Return results for SELECT queries
                connection.commit()
                return output
            connection.commit()
            return None
    except Exception as e:
        logger.error("Error executing SQL query: %s", str(e))
        raise e
    finally:
        connection.close()  
        
def update_db_records(updated_df, secrets, main_table_name, stage_table_name, update_db_query, database):
    """
    Update records in the main table based on data from a stage table using provided query.
    """
    # Extracting values from secrets
    host = secrets["host"]
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
            port=int(port)
        )
        with connection.cursor() as cursor:
            # Execute the provided query
            cursor.execute(update_db_query)
            connection.commit()
        
    
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
        

def create_preauth_batch(secrets, load_category, provider, file_name, file_path, create_update_user):
    """
    Create a new batch record in the database for preauth processing.
    
    Args:
        secrets (dict): Database connection parameters from Secrets Manager
        load_category (str): Category of load being processed
        provider (str): Provider name
        file_name (str, optional): File name
        file_path (str, optional): File prefix path
        create_update_user (str): User name for Create/Update
        
    Returns:
        int: Batch ID of the created record
    """

    v_start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    create_timestamp = v_start_time
    update_timestamp = v_start_time

    insert_sql = f"""
        INSERT INTO {BATCH_PREAUTH_MASTER_TABLE} 
            (file_type, provider, file_name, file_path, start_datetime, status, create_user, create_timestamp, update_user, update_timestamp) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    insert_values = (
        load_category,
        provider,
        file_name, 
        file_path,
        v_start_time,
        'IN PROGRESS',
        create_update_user,
        create_timestamp,
        create_update_user,
        update_timestamp
    )

    batch_id = run_insert_command(secrets, insert_sql, insert_values)
    print(f"Successfully inserted log data with batch_id: {batch_id}")
    
    return batch_id

def update_preauth_batch_status(secrets, batch_id, status, create_update_user):
    """
    Update batch status in the database for preauth processing.
    
    Args:
        secrets (dict): Database connection parameters from Secrets Manager
        batch_id (int): Batch ID to associate with the batch
        status (str): Status of the batch
        create_update_user (str): User name for Create/Update
    """

    update_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

    update_sql = f"""UPDATE {BATCH_PREAUTH_MASTER_TABLE}
                        SET status = %s,
                            end_datetime = %s,
                            update_user = %s,
                            update_timestamp = %s
                        WHERE batch_id = %s"""
    update_values = (status, update_timestamp, create_update_user, update_timestamp, batch_id)

    run_update_command(secrets, update_sql, update_values)
    logger.info(f"Successfully updated batch_id {batch_id} to status {status}.")


def preauth_step_logging(secrets, batch_id, step_id, job_name, job_id, status, create_update_user):
    """
    Log step details for preauth batch processing.
    
    Args:
        secrets (dict): Database connection parameters from Secrets Manager
        batch_id (int): Batch ID to associate with the step
        step_id (int, optional): Existing step ID for updates, None for new steps
        job_name (str): Name of the job/step
        job_id (str): Unique identifier for the job
        status (str): Status of the step
        create_update_user (str): User name for Create/Update
        
    Returns:
        int: Step ID (for new steps) or None (for updates)
    """
    v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    
    if step_id:
        update_sql = f"""UPDATE {BATCH_PREAUTH_STEP_DETAILS_TABLE}
                         SET status = %s,
                             end_datetime = %s,
                             update_user = %s,
                             update_timestamp = %s
                         WHERE batch_id = %s AND step_id = %s"""
        update_values = (status, v_load_timestamp, create_update_user, v_load_timestamp, batch_id, step_id)

        run_update_command(secrets, update_sql, update_values)
        logger.info(f"Successfully updated step status {status} for step_id: {step_id}")

    else:
        insert_sql = f"""INSERT INTO {BATCH_PREAUTH_STEP_DETAILS_TABLE} 
                            (batch_id, job_type, job_name, job_id, status, start_datetime, end_datetime, create_user, create_timestamp, update_user, update_timestamp)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        insert_values = (batch_id, 'GLUE', job_name, job_id, status, v_load_timestamp, None, create_update_user, v_load_timestamp, create_update_user, v_load_timestamp)

        step_id = run_insert_command(secrets, insert_sql, insert_values)
        logger.info(f"Successfully inserted log data with step_id: {step_id}")

        return step_id


def preauth_error_logging(secrets, batch_id, step_id, claim_number, status, error_code, error_info, error_message, create_update_user, is_batch_failed=True):
    """
    Log error details for preauth batch processing.
    
    Args:
        secrets (dict): Database connection parameters from Secrets Manager
        batch_id (int): Batch ID associated with the error
        step_id (int, optional): Step ID associated with the error
        claim_number (str, optional): Claim number for error logging
        status (str): Status of the batch/step
        error_code (str): Error code for categorization
        error_info (str): Brief error description
        error_message (str): Detailed error message
        create_update_user (str): User name for Create/Update
        is_batch_failed (boolean, optional): Skip Step and Batch status update in case of non batch fail errors, Default True
    """
    v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    
    insert_sql = f"""INSERT INTO {BATCH_PREAUTH_ERROR_DETAILS_TABLE} 
                        (batch_id, step_id, claim_number, error_code, error_info, error_message, create_user, create_timestamp, update_user, update_timestamp)
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    insert_values = (batch_id, step_id, claim_number, error_code, error_info, error_message, create_update_user, v_load_timestamp, create_update_user, v_load_timestamp)

    error_id = run_insert_command(secrets, insert_sql, insert_values)
    logger.info(f"Successfully inserted log data with error_id: {error_id}")

    if is_batch_failed:
        logger.info(f"Batch Failure, Updating batch and step status...")

        preauth_step_logging(secrets, batch_id, step_id, None, None, status, create_update_user)

        update_preauth_batch_status(secrets, batch_id, status, create_update_user)
    else:
        logger.info(f"Non Batch Failure, skipping updating of batch and step status!!!")


def rename_s3_object(bucket_name, s3_prefix, old_file_name, new_file_name):
    """
    Rename an S3 object by copying it to a new key and deleting the original.
    
    Args:
        bucket_name (str): Name of the S3 bucket
        s3_prefix (str): S3 prefix for the object
        old_file_name (str): Current file name
        new_file_name (str): New file name
        
    Returns:
        bool: True if successful, False otherwise
    """
    s3_resource = boto3.resource('s3')

    if s3_prefix:
        if not s3_prefix.endswith('/'):
            s3_prefix += '/'
        old_key = f"{s3_prefix}{old_file_name}"
        new_key = f"{s3_prefix}{new_file_name}"
    else:
        old_key = old_file_name
        new_key = new_file_name
        
    logger.info(f"Attempting to rename S3 object in bucket '{bucket_name}':")
    logger.info(f"  From Key: '{old_key}'")
    logger.info(f"  To Key:   '{new_key}'")

    try:
        try:
            s3_resource.Object(bucket_name, old_key).load()
        except s3_resource.meta.client.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                logger.warning(f"Old object '{old_key}' not found in bucket '{bucket_name}'. Skipping rename.")
                return False
            else:
                raise

        #Copy the object to the new key
        copy_source = {
            'Bucket': bucket_name,
            'Key': old_key
        }
        s3_resource.Object(bucket_name, new_key).copy_from(CopySource=copy_source)
        logger.info(f"Successfully copied '{old_key}' to '{new_key}' in bucket '{bucket_name}'.")

        #Delete the original object
        s3_resource.Object(bucket_name, old_key).delete()
        logger.info(f"Successfully deleted original object '{old_key}' from bucket '{bucket_name}'.")
        return True
    
    except Exception as e:
        logger.error(f"Error renaming S3 object from '{old_key}' to '{new_key}' in bucket '{bucket_name}': {e}")
        return False


def log_and_email(env_profile, provider, load_category, reprocess_flag, batch_id, file_name, data_pipeline_name, data_pipeline_id, error_info, error_message, severity, sns_topic_arn, process_type="PreAuth"):
    """
    Log error and send notification email via SNS.
    
    Args:
        env_profile (str): Environment profile (DEV, QA, STG, PROD)
        provider (str): Provider name
        load_category (str): Category of load
        reprocess_flag (str): Reprocess flag value
        batch_id (int, optional): Batch ID associated with the error
        file_name (str, optional): Name of the file being processed
        data_pipeline_name (str): Step Function Name
        data_pipeline_id (str): Step Function Execution ID
        error_info (str): Brief error information
        error_message (str): Detailed error message
        severity (str): Error severity level
        sns_topic_arn (str): SNS topic ARN for notifications
        process_type (str): Type of process for email context
        
    Returns:
        dict: SNS publish response
    """
    logger.info("Inside log email method")
    sns_client=boto3.client('sns',region_name='us-east-1')
    cst=pytz.timezone('America/Chicago')
    timestamp=datetime.now(cst).strftime('%Y/%m/%d %H:%M:%S %Z%z')
    provider = provider.upper()
    load_category = load_category.replace("_", " ").title()
    
    #create email message
    message_subject = f"{env_profile} - {provider} - {load_category} - {error_info} - {severity}"
    
    message_body_parts = [
        f"Process : {provider}-{load_category} - {process_type}",
        f"Batch ID: {batch_id}" if batch_id else "Batch ID: N/A",
        f"File Name: {file_name}" if file_name else "File Name: N/A",
        f"Step Function Name: {data_pipeline_name}",
        f"Step Function ID: {data_pipeline_id}",
        f"Timestamp: {timestamp}",
        f"Environment: {env_profile}",
        f"Reprocess Flag: {reprocess_flag}",
        f"Error Info: {error_info}",
        f"Fields & Content from Error log : {error_message[:500]}"
    ]
    
    message_body = "\n".join(message_body_parts)
    logger.info(message_body)
    
    response=sns_client.publish(
       TopicArn=sns_topic_arn,
       Subject=message_subject,
       Message=message_body
       )
    return response
