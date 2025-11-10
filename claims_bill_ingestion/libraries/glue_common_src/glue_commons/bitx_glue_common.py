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
from pyspark.sql.functions import *
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bitx_glue_common")

def rename_s3_file(bucket_name,sa_file_path,new_file_name):
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=sa_file_path)
    if 'Contents' in response and response['Contents']:
        files = response['Contents']
        files_sorted = sorted(files, key=lambda x: x['LastModified'], reverse=True)
        target_file = files_sorted[0]["Key"]
        logger.info(f'Target_FILE : {target_file}')
        if target_file:
            cst=pytz.timezone('America/Chicago')
            logger.info(f"New File with CST timezone: {datetime.now(cst).strftime('%Y%m%d_%H%M%S')}")
            new_file_name = new_file_name.replace('<date_format>',datetime.now(cst).strftime('%Y%m%d_%H%M%S'))
            logger.info('new_file_name',new_file_name)
            new_file_rename=sa_file_path+new_file_name
            logger.info('new file name after rename',new_file_rename)
            copy_source = {'Bucket': bucket_name, 'Key': target_file}
            s3_client.copy_object(CopySource=copy_source, Bucket=bucket_name, Key=new_file_rename)
            s3_client.delete_object(Bucket=bucket_name, Key=target_file)
            logger.info(f"Renamed File {target_file} to {new_file_rename}")
    return True

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
        
def update_invoice_status(input_df, secrets, status_type, status, process_str, header_id, update_user="Glue - ETL - Generic"):
    """
    Generic method to update invoice status in both invoice_status and invoice_step_status tables.
    
    Args:
        input_df: DataFrame containing invoice data with 'invoice_number' column
        secrets (dict): Database connection parameters from Secrets Manager
        status_type (str): Type of status to set
        status (str): Status value to set
        process_str (str): Process String for stage table naming
        header_id (str): Header ID for stage table naming
        database (str): Database name (default: "txm_bitx")
        update_user (str): User name for update operations (default: "Glue - ETL - Generic")
        
    Returns:
        None
    """
    logger.info(f"Invoice Status table update STARTED for status_type {status_type} and status {status}")
    database="txm_bitx"
    v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    stage_suffix = f"{process_str.strip()}_{str(header_id).strip()}"
    
    # Update invoice_status table
    update_main_table_name = f"{database}.invoice_status"
    logger.info(f"update_main_table_name => {update_main_table_name}")
    
    update_stage_table_name = f"{update_main_table_name}_{stage_suffix}_stage"
    logger.info(f"update_stage_table_name => {update_stage_table_name}")
    
    update_query = f"""
        UPDATE {update_main_table_name} main
        JOIN {update_stage_table_name} stage ON main.txm_invoice_number = stage.invoice_number
        SET main.status_type = '{status_type}'
        ,main.status = '{status}'
        ,update_user = '{update_user}'
        ,update_timestamp = '{v_load_timestamp}'
    """
    
    logger.info(f"Calling update_db_records function to update {update_main_table_name} in database...")
    update_db_records(input_df, secrets, update_main_table_name, update_stage_table_name, update_query, database)
    logger.info(f"Invoice Status table update COMPLETED for status_type {status_type} and status {status}")
    
    # Update invoice_step_status table
    logger.info(f"Invoice Step Status table load STARTED for status_type {status_type} and status {status}")
    update_main_table_name = f"{database}.invoice_step_status"
    logger.info(f"update_main_table_name => {update_main_table_name}")
    
    update_stage_table_name = f"{update_main_table_name}_{stage_suffix}_stage"
    logger.info(f"update_stage_table_name => {update_stage_table_name}")
    
    insert_query = f"""
        INSERT INTO {update_main_table_name} (status_id, status_type, step_info, step_status, create_user, update_user, create_timestamp, update_timestamp)
        SELECT s.status_id, '{status_type}', '{status}', '{status}', '{update_user}', '{update_user}', '{v_load_timestamp}', '{v_load_timestamp}'
        FROM {database}.invoice_status s
        JOIN {update_stage_table_name} stage ON s.txm_invoice_number = stage.invoice_number
    """
    logger.info(f"Calling update_db_records function to insert {update_main_table_name} in database...")
    update_db_records(input_df, secrets, update_main_table_name, update_stage_table_name, insert_query, database)
    logger.info(f"Invoice Step Status table load COMPLETED for status_type {status_type} and status {status}")