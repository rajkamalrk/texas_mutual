import os
import json
import logging
import zipfile
from datetime import datetime
import io
import boto3
import base64
from botocore.exceptions import ClientError
import pytz
import pymysql

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_secret(secret_name, region_name):
    """
    Retrieves secrets from AWS Secrets Manager.
    
    Args:
        secret_name (str): Name of the secret in AWS Secrets Manager
        region_name (str): AWS region where the secret is stored
        
    Returns:
        dict: Parsed secret values as a dictionary
        
    Raises:
        ClientError: If secret retrieval fails
        json.JSONDecodeError: If secret cannot be parsed as JSON
    """
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    try:
        get_secret_value_response = client.get_secret_value(SecretId = secret_name)
    except ClientError as e:
        print("Error retrieving secret '%s': %s", secret_name, str(e))
        raise e
    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response.get('SecretString', '')
    if not secret: 
        decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
        secret = decoded_binary_secret
    return json.loads(secret)

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

def update_preauth_batch_status(secrets, batch_id, status, create_update_user):
    """
    Update batch status in the database for processing.
    
    Args:
        secrets (dict): Database connection parameters from Secrets Manager
        batch_id (int): Batch ID to associate with the batch
        status (str): Status of the batch
        create_update_user (str): User name for Create/Update
    """

    update_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

    update_sql = f"""UPDATE txm_preauth_logs.batch_preauth_master
                        SET status = %s,
                            update_user = %s,
                            update_timestamp = %s
                        WHERE batch_id = %s"""
    update_values = (status, create_update_user, update_timestamp, batch_id)

    run_update_command(secrets, update_sql, update_values)
    print(f"Successfully updated batch_id {batch_id} to status {status}.")

def preauth_step_logging(secrets, batch_id, step_id, job_name, job_id, status, create_update_user):
    """
    Log step details for batch processing.
    
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
        update_sql = f"""UPDATE txm_preauth_logs.batch_preauth_step_details
                         SET status = %s,
                             end_datetime = %s,
                             update_user = %s,
                             update_timestamp = %s
                         WHERE batch_id = %s AND step_id = %s"""
        update_values = (status, v_load_timestamp, create_update_user, v_load_timestamp, batch_id, step_id)

        run_update_command(secrets, update_sql, update_values)
        logger.info(f"Successfully updated step status {status} for step_id: {step_id}")

    else:
        insert_sql = f"""INSERT INTO txm_preauth_logs.batch_preauth_step_details 
                            (batch_id, job_type, job_name, job_id, status, start_datetime, end_datetime, create_user, create_timestamp, update_user, update_timestamp)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        insert_values = (batch_id, 'LAMBDA', job_name, job_id, status, v_load_timestamp, None, create_update_user, v_load_timestamp, create_update_user, v_load_timestamp)

        step_id = run_insert_command(secrets, insert_sql, insert_values)
        logger.info(f"Successfully inserted log data with step_id: {step_id}")

        return step_id
    
def preauth_error_logging(secrets, batch_id, step_id, status, error_code, error_info, error_message, create_update_user):
    """
    Log error details for batch processing.
    
    Args:
        secrets (dict): Database connection parameters from Secrets Manager
        batch_id (int): Batch ID associated with the error
        step_id (int, optional): Step ID associated with the error
        status (str): Status of the batch/step
        error_code (str): Error code for categorization
        error_info (str): Brief error description
        error_message (str): Detailed error message
        create_update_user (str): User name for Create/Update
    """
    v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    
    insert_sql = f"""INSERT INTO txm_preauth_logs.batch_preauth_error_details 
                        (batch_id, step_id, error_code, error_info, error_message, create_user, create_timestamp, update_user, update_timestamp)
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    insert_values = (batch_id, step_id, error_code, error_info, error_message, create_update_user, v_load_timestamp, create_update_user, v_load_timestamp)

    error_id = run_insert_command(secrets, insert_sql, insert_values)
    print(f"Successfully inserted log data with error_id: {error_id}")

    preauth_step_logging(secrets, batch_id, step_id, None, None, status, create_update_user)

    update_preauth_batch_status(secrets, batch_id, status, create_update_user)

def move_input_file(folderType, bucket_name, file_prefix, s3_client):
    destination_key = file_prefix.replace('inbound', folderType)

    try:
        s3_client.copy_object(Bucket=bucket_name, Key=destination_key, CopySource={'Bucket': bucket_name, 'Key': file_prefix})
        logger.info("File copied successfully.")

        s3_client.delete_object(Bucket=bucket_name, Key=file_prefix)
        logger.info("Original file deleted successfully.")

    except Exception as e:
        logger.error(f"Error moving file: {e}")
        raise

def snstopicnotification(batch_id, provider, load_category, data_pipeline_name, data_pipeline_id, error_info, error_message, file_name, env_profile, reprocess_flag, severity, sns_topic_arn):
    logger.info(f"Attempting to send SNS notification to {sns_topic_arn}")
    cst = pytz.timezone('America/Chicago')
    timestamp = datetime.now(cst).strftime('%m/%d/%Y %H:%M:%S%Z')

    try:
        sns_client = boto3.client('sns')
        message_subject = f"{env_profile} - Unsupported File Type - {provider}-{load_category} file is Unsupported - {severity}"
        message_body = (
            f"Process: {provider} - PreAuth {load_category} file \n"
            f"Batch ID: {batch_id}\n"
            f"File Name: {file_name}\n"
            f"Step Function Name: {data_pipeline_name}\n"
            f"Step Function ID: {data_pipeline_id}\n"
            f"Timestamp: {timestamp}\n"
            f"Environment: {env_profile}\n" 
            f"Reprocess Flag: {reprocess_flag}\n" 
            f"Error Info: {error_info}\n"
            f"Fields & Content from Error log : {error_message[:500]}"
        )
        logger.info(message_body)
        sns_client.publish(
            TopicArn=sns_topic_arn,
            Subject=message_subject,
            Message=message_body
        )
        logger.info(f"Successfully sent SNS notification for {file_name}")
    except Exception as e:
        logger.error(f"Error sending SNS notification for {file_name}: {e}", exc_info=True)

def process_non_zip_file(batch_id, step_id, provider, load_category, bucket_name, nonzip_file_path, destination_folder, step_function_info, step_function_execution_id, environment, environment_map, config_values, reprocess_flag, sns_topic_arn, s3_client, secrets):
    error_info = ''
    error_message = ''

    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=nonzip_file_path)
        file_data_bytes = response['Body'].read()
        logger.info(f"Downloaded {len(file_data_bytes)} bytes for {nonzip_file_path}")

        file_name = os.path.basename(nonzip_file_path)
        file_name_wo_ext = os.path.splitext(file_name)[0]
        file_ext = os.path.splitext(file_name)[1][1:].lower()

        if not file_data_bytes:
            status = 'FAILED'
            error_info = 'Input file is empty'
            error_message = 'txt file is empty.'
            logger.error(f"{error_info}: {nonzip_file_path}")
            move_input_file('error', bucket_name, nonzip_file_path, s3_client)
            snstopicnotification(batch_id, provider, load_category, step_function_info, step_function_execution_id, error_info, error_message, file_name, environment, reprocess_flag, 'FATAL', sns_topic_arn)
            preauth_error_logging(secrets, batch_id, step_id, status, 'PREAUTH UNZIP LAMBDA FAILED', error_info, error_message, 'Lambda - Preauth Unzip Lambda')
            logger.info(f"[FAILURE] Logged failure for batch_id {batch_id} and step_id {step_id}")
            return {"statusCode": 500, "message": error_message}
    except s3_client.exceptions.NoSuchKey:
        status = 'FAILED'
        error_info = 'File not found in S3'
        error_message = f'The specified object s3://{bucket_name}/{nonzip_file_path} does not exist.'
        logger.error(error_message)
        snstopicnotification(batch_id, provider, load_category, step_function_info, step_function_execution_id, error_info, error_message, file_name, environment, reprocess_flag, 'FATAL', sns_topic_arn)
        preauth_error_logging(secrets, batch_id, step_id, status, 'PREAUTH UNZIP LAMBDA FAILED', error_info, error_message, 'Lambda - Preauth Unzip Lambda')
        logger.info(f"[FAILURE] Logged failure for batch_id {batch_id} and step_id {step_id}")
        return {"statusCode": 404, "message": error_message}
    except Exception as e:
        logger.error(f"Error fetching object {nonzip_file_path} from S3: {e}", exc_info=True)
        status = 'FAILED'
        error_info = 'S3 Download Error'
        error_message = f'Failed to download file from S3: {e}'
        move_input_file('error', bucket_name, nonzip_file_path, s3_client)
        snstopicnotification(batch_id, provider, load_category, step_function_info, step_function_execution_id, error_info, error_message, file_name, environment, reprocess_flag, 'FATAL', sns_topic_arn)
        preauth_error_logging(secrets, batch_id, step_id, status, 'PREAUTH UNZIP LAMBDA FAILED', error_info, error_message, 'Lambda - Preauth Unzip Lambda')
        logger.info(f"[FAILURE] Logged failure for batch_id {batch_id} and step_id {step_id}")
        return {"statusCode": 500, "message": error_message}
    
    if file_ext in config_values.get('FileExtn', []):
        logger.info(f"Processing {config_values.get('FileExtn', [])} file: {nonzip_file_path}")
        destination_path = (f"{destination_folder}/{provider}/preauth_code_files/{load_category}/")
        final_dest_path = f"{destination_path}{file_name}"

        logger.info(f"Uploading extracted file to: s3://{bucket_name}/{destination_path}{file_name}")
        s3_client.put_object(
            Bucket=bucket_name,
            Key=final_dest_path,
            Body=file_data_bytes
        )

        logger.info("All extracted files uploaded successfully.")
        move_input_file('archive', bucket_name, nonzip_file_path, s3_client)

    else:
        status = 'FAILED'
        error_info = 'Unsupported File Type'
        error_message = f"Unsupported file type '{file_ext}'. Only {config_values.get('FileExtn', [])} files are expected for this process."
        logger.error(f"{error_info}: {nonzip_file_path}")
        move_input_file('error', bucket_name, nonzip_file_path, s3_client)
        snstopicnotification(batch_id, provider, load_category, step_function_info, step_function_execution_id, error_info, error_message, file_name, environment, reprocess_flag, 'FATAL', sns_topic_arn)
        preauth_error_logging(secrets, batch_id, step_id, status, 'PREAUTH UNZIP LAMBDA FAILED', error_info, error_message, 'Lambda - Preauth Unzip Lambda')
        return {"statusCode": 500, "message": error_message}

    logger.info(f"Function completed successfully for {nonzip_file_path}. Main provider folder key: {destination_path}")
    return {"statusCode": 200, "message": final_dest_path}

def process_zip_file(batch_id, step_id, provider, load_category, bucket_name, zip_file_path, destination_folder, step_function_info, step_function_execution_id, environment, environment_map, config_values, reprocess_flag, sns_topic_arn, s3_client, secrets):
    error_info = ''
    error_message = ''

    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=zip_file_path)
        file_data_bytes = response['Body'].read()
        logger.info(f"Downloaded {len(file_data_bytes)} bytes for {zip_file_path}")

        folder_path_parts = zip_file_path.split('/')
        file_name = folder_path_parts[-1]
        file_name_wo_ext = os.path.splitext(file_name)[0]
        file_ext = os.path.splitext(file_name)[1][1:].lower()

        if not file_data_bytes:
            status = 'FAILED'
            error_info = 'Input file is empty'
            error_message = 'zip file is empty.'
            logger.error(f"{error_info}: {zip_file_path}")
            move_input_file('error', bucket_name, zip_file_path, s3_client)
            snstopicnotification(batch_id, provider, load_category, step_function_info, step_function_execution_id, error_info, error_message, file_name, environment, reprocess_flag, 'FATAL', sns_topic_arn)
            preauth_error_logging(secrets, batch_id, step_id, status, 'PREAUTH UNZIP LAMBDA FAILED', error_info, error_message, 'Lambda - Preauth Unzip Lambda')
            return {"statusCode": 500, "message": error_message}
    except s3_client.exceptions.NoSuchKey:
        status = 'FAILED'
        error_info = 'File not found in S3'
        error_message = f'The specified object s3://{bucket_name}/{zip_file_path} does not exist.'
        logger.error(error_message)
        snstopicnotification(batch_id, provider, load_category, step_function_info, step_function_execution_id, error_info, error_message, file_name, environment, reprocess_flag, 'FATAL', sns_topic_arn)
        preauth_error_logging(secrets, batch_id, step_id, status, 'PREAUTH UNZIP LAMBDA FAILED', error_info, error_message, 'Lambda - Preauth Unzip Lambda')
        return {"statusCode": 404, "message": error_message}
    except Exception as e:
        logger.error(f"Error fetching object {zip_file_path} from S3: {e}", exc_info=True)
        status = 'FAILED'
        error_info = 'S3 Download Error'
        error_message = f'Failed to download file from S3: {e}'
        move_input_file('error', bucket_name, zip_file_path, s3_client)
        snstopicnotification(batch_id, provider, load_category, step_function_info, step_function_execution_id, error_info, error_message, file_name, environment, reprocess_flag, 'FATAL', sns_topic_arn)
        preauth_error_logging(secrets, batch_id, step_id, status, 'PREAUTH UNZIP LAMBDA FAILED', error_info, error_message, 'Lambda - Preauth Unzip Lambda')
        return {"statusCode": 500, "message": error_message}

    if file_ext in environment_map.get('unzipExtn', []):
        logger.info(f"Processing ZIP file: {zip_file_path}")
        try:
            with zipfile.ZipFile(io.BytesIO(file_data_bytes), 'r') as zf:
                zip_entries = zf.infolist()

                if not zip_entries:
                    status = 'FAILED'
                    error_info = 'Zip file is Empty'
                    error_message = 'The provided ZIP file contains no entries.'
                    logger.error(f"{error_info}: {zip_file_path}")
                    move_input_file('error', bucket_name, zip_file_path, s3_client)
                    snstopicnotification(batch_id, provider, load_category, step_function_info, step_function_execution_id, error_info, error_message, file_name, environment, reprocess_flag, 'FATAL', sns_topic_arn)
                    preauth_error_logging(secrets, batch_id, step_id, status, 'PREAUTH UNZIP LAMBDA FAILED', error_info, error_message, 'Lambda - Preauth Unzip Lambda')
                    return {"statusCode": 500, "message": error_message}

                for entry in zip_entries:
                    if not entry.is_dir():
                        entry_data_name = entry.filename
                        logger.info(f"entry_data_name: {entry_data_name}")
                        # Check if the extracted file's extension is in the allowed list for the provider
                        extracted_file_ext = os.path.splitext(entry_data_name)[1][1:].lower()
                        if extracted_file_ext in config_values.get('files', []):
                            destination_path = (f"{destination_folder}/{provider}/preauth_code_files/{load_category}/{file_name_wo_ext}/")
                            destination_path_in_s3 = (f"{destination_folder}/{provider}/preauth_code_files/{load_category}/{file_name_wo_ext}/{entry_data_name}")

                            extracted_file_data = zf.read(entry)
                            logger.info(f"Uploading extracted file to: s3://{bucket_name}/{destination_path_in_s3}")
                            s3_client.put_object(
                                Bucket=bucket_name,
                                Key=destination_path_in_s3,
                                Body=extracted_file_data
                            )
                        
                logger.info("All extracted files uploaded successfully.")
                move_input_file('archive', bucket_name, zip_file_path, s3_client)

        except zipfile.BadZipFile as e:
            status = 'FAILED'
            error_info = 'Invalid/corrupt zip file.'
            error_message = f'The input file is a corrupted or invalid ZIP file: {e}'
            logger.error(f"{error_info}: {zip_file_path}, Error: {e}", exc_info=True)
            move_input_file('error', bucket_name, zip_file_path, s3_client)
            snstopicnotification(batch_id, provider, load_category, step_function_info, step_function_execution_id, error_info, error_message, file_name, environment, reprocess_flag, 'FATAL', sns_topic_arn)
            preauth_error_logging(secrets, batch_id, step_id, status, 'PREAUTH UNZIP LAMBDA FAILED', error_info, error_message, 'Lambda - Preauth Unzip Lambda')
            return {"statusCode": 500, "message": error_message}
        except Exception as e:
            logger.error(f"Error during ZIP file processing for {zip_file_path}: {e}", exc_info=True)
            status = 'FAILED'
            error_info = 'ZIP Processing Error'
            error_message = f'An unexpected error occurred during ZIP processing: {e}'
            move_input_file('error', bucket_name, zip_file_path, s3_client)
            snstopicnotification(batch_id, provider, load_category, step_function_info, step_function_execution_id, error_info, error_message, file_name, environment, reprocess_flag, 'FATAL', sns_topic_arn)
            preauth_error_logging(secrets, batch_id, step_id, status, 'PREAUTH UNZIP LAMBDA FAILED', error_info, error_message, 'Lambda - Preauth Unzip Lambda')
            return {"statusCode": 500, "message": error_message}

    else:
        status = 'FAILED'
        error_info = 'Unsupported File Type'
        error_message = f"Unsupported file type '{file_ext}'. Only ZIP files are expected for this process."
        logger.error(f"{error_info}: {zip_file_path}")
        move_input_file('error', bucket_name, zip_file_path, s3_client)
        snstopicnotification(batch_id, provider, load_category, step_function_info, step_function_execution_id, error_info, error_message, file_name, environment, reprocess_flag, 'FATAL', sns_topic_arn)
        preauth_error_logging(secrets, batch_id, step_id, status, 'PREAUTH UNZIP LAMBDA FAILED', error_info, error_message, 'Lambda - Preauth Unzip Lambda')
        return {"statusCode": 500, "message": error_message}

    logger.info(f"Function completed successfully for {zip_file_path}. Main provider folder key: {destination_path}")
    return {"statusCode": 200, "message": destination_path}

def return_response(error_info, error_message, provider, load_category, s3_bucket, file_name, step_function_info, reprocess_flag, batch_id, step_function_execution_id, status_code, request_id, lambda_name):
    logger.info(f"Stopping the process: {error_message}")
    body_content = {
        "lambdaName": lambda_name,
        "parsedBody": {
            "step_function_info": step_function_info,
            "reprocess_flag": reprocess_flag,
            "provider": provider,
            'load_category' : load_category,
            "batch_id": batch_id,
            "step_function_execution_id": step_function_execution_id,
            "s3_bucket": s3_bucket,
            'file_name': file_name
        },
        "errorInfo": error_info,
        "errorMessage": error_message
    }

    return {
        "statusCode": status_code,
        "requestId": request_id,
        "lambdaName": lambda_name,
        "body": json.dumps(body_content)
    }

def run_sql_query(db_sql,secrets):
    output = None
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
            print('Query executed successfully')
            return output
    finally:
        connection.close()


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
        
        print(f"Calling DB Stored Procedure {db_stored_procedure}...")
        print(f"db_sp_params => {db_sp_params}")
        #cursor.execute('SET SESSION max_execution_time = 3600')
        cursor.execute('SET LOCAL net_read_timeout = 300')
        #cursor.execute('SET LOCAL net_read_timeout = 31536000')
        #cursor.execute('SET LOCAL wait_timeout = 28800')
        #cursor.execute('SET LOCAL interactive_timeout = 28800')
        cur_res = cursor.callproc(db_stored_procedure, db_sp_params)
        print(f"cur_res => {cur_res}")
        print(f"Stored Procedure Executed!!!")
        print(f"Stored Procedure execution output:")
        print(cursor.fetchall()) ###First resultset
        while cursor.nextset(): ###Remaining resultset
            for res in cursor.fetchall():
                print(res)
                if any(str.startswith('ProcedureExecutionError:') for str in res):
                    raise Exception(f"Error while executing Stored Procedure {db_stored_procedure}!!!")
        if return_out_param_position:
            out_param_query = f'SELECT @_{db_stored_procedure}_{return_out_param_position}'
            print(f"out_param_query => {out_param_query}")
            cur_res = cursor.execute(out_param_query)
            print(f"cur_res => {cur_res}")
            out_res = cursor.fetchone() ###First resultset
            print(f"out_res => {out_res}")
            if out_res:
                out_param_value = out_res[0]
        print(f"out_param_value => {out_param_value}")
        conn.commit()
    except Exception as e:
        print("Error executing DB Stored Procedure: %s", str(e))
        raise e
    finally:
        conn.close()
    return out_param_value  

def lambda_handler(event, context):
    logger.info('Inside handler')
    logger.info(f"Received event: {json.dumps(event)}")
    logger.info(f"Reeived context : {context}")
    aws_request_id = context.aws_request_id
    lambda_name = context.function_name
    initiate_folder = os.environ.get('initiate_folder')
    destination_folder = os.environ.get('Destination_folder')
    preauth_config_str = os.environ.get('preauth_config')  
    sns_topic_arn = os.environ.get('sns_topic_arn')
    db_secret_key = os.environ['db_secret_key']

    batch_id = None
    step_id = None
    status = ""
    provider = ""
    load_category = ""
    step_function_info = ""
    step_function_execution_id = ""
    reprocess_flag = ""

    try:
        status = "COMPLETED"

        # Extract details from the event body
        parsed_body = event.get('parsedBody', {})

        provider = parsed_body.get('provider')
        reprocess_flag = parsed_body.get('reprocess_flag')
        load_category = parsed_body.get('load_category')
        batch_id = parsed_body.get('batch_id')
        step_function_info = parsed_body.get('step_function_info')
        step_function_execution_id = parsed_body.get('step_function_execution_id')
        bucket_name = parsed_body.get('s3_bucket')
        file_name = parsed_body.get('file_name')
        failed_batch_id = parsed_body.get('failed_batch_id')
        is_execution=True
        is_reprocess_exec=False 
        logger.info(f"db_secret_key => {db_secret_key}")
        reprocess_valid_list=['completed','completed_in_reprocess','skip_execution']
        logger.info(f"provider value: {provider}")
        logger.info(f"Bucket name: {bucket_name}")
        logger.info(f"AWS Request ID value: {aws_request_id}")
        logger.info(f"Lambda Name value: {lambda_name}")
        logger.info(f"Reprocess flag value: {reprocess_flag}")
        logger.info(f"failed_batch_id : {failed_batch_id}")
        logger.info(f"Batch ID value: {batch_id}")
        logger.info(f"load_category value: {load_category}")
        logger.info(f"PreAuth File Name: {file_name}")
        
        s3_client = boto3.client('s3')

        secrets = get_secret(db_secret_key, 'us-east-1')

        if batch_id:
            step_id = preauth_step_logging(secrets, batch_id, step_id, lambda_name, aws_request_id, 'IN PROGRESS', 'Lambda - Preauth Unzip Lambda')
            logger.info(f"step_id => {step_id}")
        
        else:
            raise ValueError(f"Batch ID {batch_id} is empty!!!")
        
        zip_file_path = None
        nonzip_file_path = None
        file_name_reprocess=''
        if reprocess_flag.upper().strip()=='Y' :
            validate_unzip_sql=f"""SELECT  m.batch_id,st.status,st.step_id,m.file_name,file_type FROM txm_preauth_logs.batch_preauth_step_details st
                        INNER JOIN   txm_preauth_logs.batch_preauth_master m ON m.batch_id=st.batch_id
                        WHERE m.batch_id={failed_batch_id}  AND job_name='{lambda_name}' """
            validation_unzip_status=run_sql_query(validate_unzip_sql, secrets)
            logger.info(f"validation_unzip_status  => {validation_unzip_status}")
            if validation_unzip_status:
                file_name_reprocess=validation_unzip_status[0]['file_name']
                if load_category.lower() == 'letters':
                    if isinstance(validation_unzip_status, list):
                        if str(validation_unzip_status[0]['status']).lower() in reprocess_valid_list:
                            print(f"Batch=> {failed_batch_id} with file name =>{validation_unzip_status[0]['file_name']} is already processed succesfully in the last run.Terminating unzip Reprocess!")
                            is_execution=False
                            v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                            update_current_batch_id_stat=f"""UPDATE txm_preauth_logs.batch_preauth_step_details  st
                                            INNER JOIN   txm_preauth_logs.batch_preauth_master m ON m.batch_id=st.batch_id
                                            SET st.status='SKIP_EXECUTION',st.end_datetime='{v_load_timestamp}'
                                            WHERE m.batch_id={batch_id}  AND job_name='{lambda_name}'"""
                            run_sql_query(update_current_batch_id_stat, secrets)
                            
                        else:
                            s3_input_files_prefix=f"{file_name.replace('inbound','error')}"
                            destination_prefix=f"{file_name}"
                            logger.info(f"destination_prefix variable=>{destination_prefix}")    
                            logger.info(f"s3_input_files_prefix variable =>{s3_input_files_prefix}")
                            s3_client = boto3.client("s3")
                            response=s3_client.copy_object(Bucket=bucket_name, Key=destination_prefix, CopySource={'Bucket': bucket_name, 'Key': s3_input_files_prefix})
                            logger.info("File copied successfully.")
                            logger.info(f"File Moved from Error to letters Folder=>{response}")
                            is_execution=True
                            is_reprocess_exec=True
                            s3_client.delete_object(Bucket=bucket_name,Key=f"{s3_input_files_prefix}")
                elif load_category.lower() == 'notes':
                    logger.info("Reprocess for Notes!")
                    if isinstance(validation_unzip_status, list):
                        if str(validation_unzip_status[0]['status']).lower() in reprocess_valid_list:
                            print(f"Batch=> {failed_batch_id} with file name =>{validation_unzip_status[0]['file_name']} is already processed succesfully in the last run.Terminating unzip Reprocess!")
                            is_execution=False
                            v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                            update_current_batch_id_stat=f"""UPDATE txm_preauth_logs.batch_preauth_step_details  st
                                            INNER JOIN   txm_preauth_logs.batch_preauth_master m ON m.batch_id=st.batch_id
                                            SET st.status='SKIP_EXECUTION',st.end_datetime='{v_load_timestamp}'
                                            WHERE m.batch_id={batch_id}  AND job_name='{lambda_name}'"""
                            run_sql_query(update_current_batch_id_stat, secrets)
                        else:
                            s3_input_files_prefix=f"{file_name.replace('inbound','error')}"
                            destination_prefix=f"{file_name}"
                            logger.info(f"destination_prefix variable=>{destination_prefix}")    
                            logger.info(f"s3_input_files_prefix variable =>{s3_input_files_prefix}")
                            s3_client = boto3.client("s3")
                            response=s3_client.copy_object(Bucket=bucket_name, Key=destination_prefix, CopySource={'Bucket': bucket_name, 'Key': s3_input_files_prefix})
                            logger.info(f"File Moved from Error to preauth Folder=>{response}")
                            is_execution=True
                            is_reprocess_exec=True 
                            s3_client.delete_object(Bucket=bucket_name,Key=f"{s3_input_files_prefix}")
                else:
                    raise Exception('Invalid Load Category for Reprocess!')
        try:
            environment_map = json.loads(preauth_config_str)
            environment = environment_map.get('env')
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing preauth_config JSON: {e}")
            status = 'FAILED'
            error_info = 'Configuration Error'
            error_message = f'preauth_config environment variable is invalid JSON: {e}'
            snstopicnotification(batch_id, provider, load_category, step_function_info, step_function_execution_id, error_info, error_message, 'N/A', 'unknown', reprocess_flag, 'FATAL', sns_topic_arn)
            preauth_error_logging(secrets, batch_id, step_id, status, 'PREAUTH UNZIP LAMBDA FAILED', error_info, error_message, 'Lambda - Preauth Unzip Lambda')
            return {"statusCode": 500, "message": error_message}
        if is_execution:
            if not destination_folder:
                logger.error("Environment variable 'Destination_folder' is not set.")
                status = 'FAILED'
                error_info = 'Configuration Error'
                error_message = 'Destination_folder environment variable is missing.'
                snstopicnotification(batch_id, provider, load_category, step_function_info, step_function_execution_id, error_info, error_message, file_name, environment, reprocess_flag, 'FATAL', sns_topic_arn)
                preauth_error_logging(secrets, batch_id, step_id, status, 'PREAUTH UNZIP LAMBDA FAILED', error_info, error_message, 'Lambda - Preauth Unzip Lambda')
                status_code = 500
                return return_response(error_info, error_message, provider, load_category, bucket_name, file_name, step_function_info, reprocess_flag, batch_id, step_function_execution_id, status_code, aws_request_id, lambda_name)
            
            if not preauth_config_str:
                logger.error("Environment variable 'preauth_config' is not set.")
                status = 'FAILED'
                error_info = 'Configuration Error'
                error_message = 'preauth_config environment variable is missing.'
                snstopicnotification(batch_id, provider, load_category, step_function_info, step_function_execution_id, error_info, error_message, file_name, environment, reprocess_flag, 'FATAL', sns_topic_arn)
                preauth_error_logging(secrets, batch_id, step_id, status, 'PREAUTH UNZIP LAMBDA FAILED', error_info, error_message, 'Lambda - Preauth Unzip Lambda')
                status_code = 500
                return return_response(error_info, error_message, provider, load_category, bucket_name, file_name, step_function_info, reprocess_flag, batch_id, step_function_execution_id, status_code, aws_request_id, lambda_name)
            
            config_values = environment_map.get(load_category)
            if not config_values:
                status = 'FAILED'
                error_info = f'Configuration not found for trading partner: {provider}'
                error_message = f'preauth_config missing entry for {provider}'
                logger.error(error_message)
                snstopicnotification(batch_id, provider, load_category, step_function_info, step_function_execution_id, error_info, error_message, file_name, environment, reprocess_flag, 'FATAL', sns_topic_arn)
                preauth_error_logging(secrets, batch_id, step_id, status, 'PREAUTH UNZIP LAMBDA FAILED', error_info, error_message, 'Lambda - Preauth Unzip Lambda')
                status_code = 500
                return return_response(error_info, error_message, provider, load_category, bucket_name, file_name, step_function_info, reprocess_flag, batch_id, step_function_execution_id, status_code, aws_request_id, lambda_name)

            if load_category == 'notes':
                nonzip_file_path = file_name
                file_name = nonzip_file_path.split("/")[-1]
                file_name_wo_ext = os.path.splitext(file_name)[0]
                parts = file_name_wo_ext.split('_')
                start_concat_parts = (f"{parts[0]}_")
                logger.info(f"start concat parts : {start_concat_parts}")
                end_concat_parts = (f"_{parts[-2]}_{parts[-1]}")
                logger.info(f"end concat parts : {start_concat_parts}")

                if start_concat_parts in config_values.get('file_starts') and end_concat_parts in config_values.get('file_ends'):  
                    logger.info(f"correct nonzip files : {nonzip_file_path}")
                else:
                    status = 'FAILED'
                    error_info = 'Unsupported file type'
                    error_message = 'The input file name format is invalid'
                    logger.error(error_message)
                    snstopicnotification(batch_id, provider, load_category, step_function_info, step_function_execution_id, error_info, error_message, file_name, environment, reprocess_flag, 'FATAL', sns_topic_arn)
                    preauth_error_logging(secrets, batch_id, step_id, status, 'PREAUTH UNZIP LAMBDA FAILED', error_info, error_message, 'Lambda - Preauth Unzip Lambda')
                    status_code = 500
                    return return_response(error_info,error_message,provider,load_category,bucket_name,file_name,step_function_info,reprocess_flag,batch_id,step_function_execution_id,status_code,aws_request_id,lambda_name)
                
                logger.info(f'Processing file: {nonzip_file_path}')
                process_file_result = process_non_zip_file(batch_id, step_id, provider, load_category, bucket_name, nonzip_file_path, destination_folder, step_function_info, step_function_execution_id, environment, environment_map, config_values, reprocess_flag, sns_topic_arn, s3_client, secrets)
                logger.info(f"Processed file result for {nonzip_file_path}: {process_file_result}")

            if load_category == 'letters':
                zip_file_path = file_name
                parts = zip_file_path.split("/")[-1].split('_')
                concat_parts = (f"{parts[0]}_{parts[1]}_")
                logger.info(f"concat parts : {concat_parts}")

                # Checks the lenght of file name and constant value (TMI_IDX or TMI_IMG)  # file name TMI_IDX_20250331_170112
                if len(parts) == 4 and concat_parts in config_values.get('file_starts'):
                    logger.info(f"correct zip files : {zip_file_path}")
                else:
                    error_info = 'Unsupported file type'
                    error_message = 'The input file name format is invalid'
                    logger.error(error_message)
                    logger.info(f"file prefix : {zip_file_path}")
                    move_input_file('error', bucket_name, zip_file_path, s3_client)
                    snstopicnotification(batch_id, provider, load_category, step_function_info, step_function_execution_id, error_info, error_message, file_name, environment, reprocess_flag, 'FATAL', sns_topic_arn)
                    preauth_error_logging(secrets, batch_id, step_id, status, 'PREAUTH UNZIP LAMBDA FAILED', error_info, error_message, 'Lambda - Preauth Unzip Lambda')
                    status_code = 500
                    return return_response(error_info,error_message,provider,load_category,bucket_name,file_name,step_function_info,reprocess_flag,batch_id,step_function_execution_id,status_code,aws_request_id,lambda_name)

                logger.info(f'Unzipping and processing file: {zip_file_path}')
                process_file_result = process_zip_file(batch_id, step_id, provider, load_category, bucket_name, zip_file_path, destination_folder, step_function_info, step_function_execution_id, environment, environment_map, config_values, reprocess_flag, sns_topic_arn, s3_client, secrets)
                logger.info(f"Unzipped file result for {zip_file_path}: {process_file_result}")

            if process_file_result['statusCode'] == 500:
                status = 'FAILED'
                event['errorInfo'] = process_file_result.get('errorInfo', 'Error during file processing')
                event['errorMessage'] = process_file_result['message']
                return {
                    "statusCode": 500,
                    "requestId": aws_request_id,
                    "lambdaName": lambda_name,
                    "body": json.dumps(event)
                }
                # Continue processing other files even if one fails
            elif process_file_result['statusCode'] == 404:
                status = 'FAILED'
                event['errorInfo'] = 'File not found in S3'
                event['errorMessage'] = process_file_result['message']
                return {
                    "statusCode": 404,
                    "requestId": aws_request_id,
                    "lambdaName": lambda_name,
                    "body": json.dumps(event)
                }
            else:
                preauth_step_logging(secrets, batch_id, step_id, None, None, status, 'Lambda - Preauth Unzip Lambda')
                logger.info(f"[SUCCESS] Successfully updated step_id {step_id} to status {status}.")
                if is_reprocess_exec:
                    logger.info(f"Reprocessing file is_reprocess_exec status: {is_reprocess_exec}")
                    update_repro_stat=f"""  UPDATE txm_preauth_logs.batch_preauth_step_details  st
                                            INNER JOIN   txm_preauth_logs.batch_preauth_master m ON m.batch_id=st.batch_id
                                            SET st.status='COMPLETED_IN_REPROCESS'
                                            WHERE m.batch_id={failed_batch_id}  AND job_name='{lambda_name}' """
                    logger.info(f"Reprocessing file==> update_repro_stat: {update_repro_stat}")                       
                    run_sql_query(update_repro_stat, secrets)
                    
                # Success response
                return {
                    "statusCode": 200,
                    "requestId": aws_request_id,
                    "lambdaName": lambda_name,
                    "body": json.dumps({
                        "message": 'All valid files processed successfully',
                        "provider": provider,
                        "load_category": load_category,
                        "s3_bucket": bucket_name,
                        "input_file_name": os.path.basename(file_name),
                        "s3_input_files_prefix": process_file_result['message'], 
                        "step_function_info": step_function_info,
                        "reprocess_flag": reprocess_flag,
                        "batch_id": batch_id,
                        "step_function_execution_id": step_function_execution_id,
                        "create_date": datetime.now(pytz.timezone('America/Chicago')).strftime("%Y-%m-%d"),
                        "failed_batch_id":failed_batch_id
                    })
                }
        else:
            # Success response
            if load_category.lower() == 'letters':
                file_name_reprocess=file_name_reprocess.replace('.zip','')
                s3_input_files_prefix=f'data/source/preauth/genex/preauth_code_files/letters/{file_name_reprocess}/'
            else:
                s3_input_files_prefix=f'data/source/preauth/genex/preauth_code_files/notes/{file_name_reprocess}'
    
            return {
                "statusCode": 200,
                "requestId": aws_request_id,
                "lambdaName": lambda_name,
                "body": json.dumps({
                    "message": 'All valid files processed successfully',
                    "provider": provider,
                    "load_category": load_category,
                    "s3_bucket": bucket_name,
                    "input_file_name": os.path.basename(file_name),
                    "s3_input_files_prefix": s3_input_files_prefix,
                    "step_function_info": step_function_info,
                    "reprocess_flag": reprocess_flag,
                    "batch_id": batch_id,
                    "step_function_execution_id": step_function_execution_id,
                    "create_date": datetime.now(pytz.timezone('America/Chicago')).strftime("%Y-%m-%d"),
                    "failed_batch_id":failed_batch_id
                })
            }   
        

    except Exception as error:
        logger.error(f"Exception Error in handler: {error}", exc_info=True)
        status = 'FAILED'
        event['error_info'] = 'Error processing the uploaded file'
        event['error_message'] = str(error)
        snstopicnotification(batch_id, provider, load_category, step_function_info, step_function_execution_id, event['error_info'], event['error_message'], file_name, environment, reprocess_flag, 'FATAL', sns_topic_arn)
        if batch_id:
            preauth_error_logging(secrets, batch_id, step_id, status, 'PREAUTH UNZIP LAMBDA FAILED', event['error_info'], event['error_message'], 'Lambda - PreAuth Unzip Lambda')
            logger.info(f"[FAILURE] Logged failure for batch_id {batch_id} and step_id {step_id}")
        return {
            "statusCode": 500,
            "requestId": aws_request_id,
            "lambdaName": lambda_name,
            "body": json.dumps(event)
        }