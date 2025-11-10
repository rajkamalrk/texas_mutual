import sys
import logging
import json
import base64
import boto3
import pytz
import pymysql
from datetime import datetime
from botocore.exceptions import ClientError
from pyspark.sql.functions import lit, col

# Import from bitx_glue_common module
from .bitx_glue_common import run_insert_command, run_update_command

# Inbound feeds log table variables
INBOUND_FEEDS_LOG_SCHEMA = "txm_inbound_feeds_logs"
BATCH_INBOUND_FEEDS_MASTER_TABLE = f"{INBOUND_FEEDS_LOG_SCHEMA}.batch_inbound_feeds_master"
BATCH_INBOUND_FEEDS_STEP_DETAILS_TABLE = f"{INBOUND_FEEDS_LOG_SCHEMA}.batch_inbound_feeds_step_details"
BATCH_INBOUND_FEEDS_ERROR_DETAILS_TABLE = f"{INBOUND_FEEDS_LOG_SCHEMA}.batch_inbound_feeds_error_details"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bitx_feeds_glue_common")

def create_feeds_batch(secrets, feed_type, provider, file_name, file_path, create_update_user):
    """
    Create a new batch record in the database for feeds processing.
    
    Args:
        secrets (dict): Database connection parameters from Secrets Manager
        feed_type (str): Type of feed being processed
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
        INSERT INTO {BATCH_INBOUND_FEEDS_MASTER_TABLE} 
            (feed_type, provider, file_name, file_path, start_datetime, status, create_user, create_timestamp, update_user, update_timestamp) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    insert_values = (
        feed_type,
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

def update_feeds_batch_status(secrets, batch_id, status, create_update_user):
    """
    Update batch status in the database for feeds processing.
    
    Args:
        secrets (dict): Database connection parameters from Secrets Manager
        batch_id (int): Batch ID to associate with the batch
        status (str): Status of the batch
        create_update_user (str): User name for Create/Update
    """

    update_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

    update_sql = f"""UPDATE {BATCH_INBOUND_FEEDS_MASTER_TABLE}
                        SET status = %s,
                            end_datetime = %s,
                            update_user = %s,
                            update_timestamp = %s
                        WHERE batch_id = %s"""
    update_values = (status, update_timestamp, create_update_user, update_timestamp, batch_id)

    run_update_command(secrets, update_sql, update_values)
    logger.info(f"Successfully updated batch_id {batch_id} to status {status}.")


def feeds_step_logging(secrets, batch_id, step_id, job_name, job_id, status, create_update_user):
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
        update_sql = f"""UPDATE {BATCH_INBOUND_FEEDS_STEP_DETAILS_TABLE}
                         SET status = %s,
                             end_datetime = %s,
                             update_user = %s,
                             update_timestamp = %s
                         WHERE batch_id = %s AND step_id = %s"""
        update_values = (status, v_load_timestamp, create_update_user, v_load_timestamp, batch_id, step_id)

        run_update_command(secrets, update_sql, update_values)
        logger.info(f"Successfully updated step status {status} for step_id: {step_id}")

    else:
        insert_sql = f"""INSERT INTO {BATCH_INBOUND_FEEDS_STEP_DETAILS_TABLE} 
                            (batch_id, job_type, job_name, job_id, status, start_datetime, end_datetime, create_user, create_timestamp, update_user, update_timestamp)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        insert_values = (batch_id, 'GLUE', job_name, job_id, status, v_load_timestamp, None, create_update_user, v_load_timestamp, create_update_user, v_load_timestamp)

        step_id = run_insert_command(secrets, insert_sql, insert_values)
        logger.info(f"Successfully inserted log data with step_id: {step_id}")

        return step_id


def feeds_error_logging(secrets, batch_id, step_id, txm_invoice_number, status, job_name, job_id, error_code, error_info, error_message, create_update_user, is_batch_failed=True):
    """
    Log error details for batch processing.
    
    Args:
        secrets (dict): Database connection parameters from Secrets Manager
        batch_id (int): Batch ID associated with the error
        step_id (int, optional): Step ID associated with the error
        txm_invoice_number (bigint, optional): Invoice number for error logging
        status (str): Status of the batch/step
        error_code (str): Error code for categorization
        error_info (str): Brief error description
        error_message (str): Detailed error message
        create_update_user (str): User name for Create/Update
        is_batch_failed (boolean, optional): Skip Step and Batch status update in case of non batch fail errors, Default True
    """
    v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    
    insert_sql = f"""INSERT INTO {BATCH_INBOUND_FEEDS_ERROR_DETAILS_TABLE} 
                        (batch_id, step_id, txm_invoice_number, error_code, error_info, error_message, create_user, create_timestamp, update_user, update_timestamp)
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    insert_values = (batch_id, step_id, txm_invoice_number, error_code, error_info, error_message, create_update_user, v_load_timestamp, create_update_user, v_load_timestamp)

    error_id = run_insert_command(secrets, insert_sql, insert_values)
    logger.info(f"Successfully inserted log data with error_id: {error_id}")

    if is_batch_failed:
        logger.info(f"Batch Failure, Updating batch and step status...")

        feeds_step_logging(secrets, batch_id, step_id, job_name, job_id, status, create_update_user)

        update_feeds_batch_status(secrets, batch_id, status, create_update_user)
    else:
        logger.info(f"Non Batch Failure, skipping updating of batch and step status!!!")


def log_and_email(env_profile, provider, feed_type, reprocess_flag, batch_id, file_name, data_pipeline_name, data_pipeline_id, error_info, error_message, severity, sns_topic_arn, process_type="Feeds"):
    """
    Log error and send notification email via SNS.
    
    Args:
        env_profile (str): Environment profile (DEV, QA, STG, PROD)
        provider (str): Provider name
        feed_type (str): Type of feed
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
    feed_type = feed_type.replace("_", " ").title()
    
    #create email message
    message_subject = f"{env_profile} - {provider} - {feed_type} - {error_info} - {severity}"
    
    message_body_parts = [
        f"Process : {provider}-{feed_type} - {process_type}",
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


def log_and_email_attachment(error_df, process, env_profile, batch_id, feed_type, provider, bucket_name, source_prefix, target_prefix, file_name, error_alerts_to_email_ids, lambda_function_nodemailer):
    """
    Log error and send email with attachment for feeds processing.
    
    Args:
        error_df: DataFrame containing error details
        process (str): Process name for email context
        env_profile (str): Environment profile
        batch_id (int): Batch ID
        feed_type (str): Type of feed
        provider (str): Provider name
        bucket_name (str): S3 bucket name
        source_prefix (str): S3 source prefix
        target_prefix (str): S3 target prefix
        file_name (str): File name being processed
        error_alerts_to_email_ids (str): Email IDs for error alerts
        lambda_function_nodemailer (str): Lambda function for sending emails
        
    Returns:
        dict: Lambda response
    """
    logger.info("inside log email attachment method")
    try:
        current_date = datetime.now().strftime('%Y-%m-%d')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        cst_time = datetime.now(pytz.timezone('America/Chicago')).strftime('%Y/%m/%d %H:%M:%SCST')
        source_prefix_with_date = f"{source_prefix}/{current_date}/"
        target_prefix_with_date = f"{target_prefix}/{current_date}/"
        logger.info(f'source_prefix_with_date : {source_prefix_with_date}')
        logger.info(f'target_prefix_with_date : {target_prefix_with_date}')
        feed_type = feed_type.title()
        feed_type_str = feed_type.replace("_", " ").title()

        # Prepare error data for email attachment
        if 'invoice_number' in error_df.columns:
            # For feeds-deleted-invoice-load format
            failed_bills_error_log_df = error_df.withColumn('Process', lit(process)) \
                .withColumnRenamed('invoice_number', 'Invoice Number') \
                .withColumnRenamed('error_description', 'Error Description') \
                .withColumnRenamed('field_name_1', 'Field name 1') \
                .withColumnRenamed('field_value_1', 'Field Value 1') \
                .withColumnRenamed('field_name_2', 'Field Name 2') \
                .withColumnRenamed('field_value_2', 'Field Value 2') \
                .withColumn('TimeStamp', lit(cst_time)) \
                .select(col('Invoice Number'), col('Error Description'), col('Field Name 1'), col('Field Value 1'), col('Field Name 2'), col('Field Value 2'), col('TimeStamp'))
        else:
            # For feeds-inbound-enhanced-load format
            failed_bills_error_log_df = error_df.withColumn('Process', lit(process)) \
                .withColumn('Receive Date', lit('')) \
                .withColumn('Invoice Number', lit('')) \
                .withColumnRenamed('error_description','Error Description') \
                .withColumnRenamed('field_name_1','Field name 1') \
                .withColumnRenamed('field_value_1','Field Value 1') \
                .withColumnRenamed('field_name_2','Field Name 2') \
                .withColumnRenamed('field_value_2','Field Value 2') \
                .withColumn('TimeStamp', lit(cst_time)) \
                .select(col('Receive Date'),col('Invoice Number'),col('Error Description'),col('Field Name 1'),col('Field Value 1'),col('Field Name 2'),col('Field Value 2'),col('TimeStamp'))

        failed_bills_error_log_df.printSchema()
        failed_bills_error_log_df.show(10, truncate=False)
        recordCount = failed_bills_error_log_df.count()
        logger.info(f"Error Mail Record Count => {recordCount}")

        failed_bills_error_log_df.coalesce(1).write.option('header','true').mode('append').format('csv').save(f"s3://{bucket_name}/{source_prefix_with_date}")

        s3_client = boto3.client('s3','us-east-1')
        provider = provider.title()
        target_file_name = f'{provider}_{batch_id}_{feed_type}_{timestamp}.csv'
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

                # Invoke the Lambda function to send an email with the attachment
                client = boto3.client('lambda',region_name='us-east-1')
                payload = {
                    's3Bucket': bucket_name,
                    's3Key': target_file,
                    'to': error_alerts_to_email_ids,
                    'subject': f'{env_profile} - {feed_type_str} - {process}',
                    'html': f"""<p>Please find the attached error log for {feed_type_str} with batch ID {batch_id}.</p>
                                <p>Feed Type: {feed_type}</p>
                                <p>Batch ID: {batch_id}</p>
                                <p>File Name:{file_name}</p>"""
                }

                try:
                    response = client.invoke(
                        FunctionName=lambda_function_nodemailer,
                        InvocationType='RequestResponse',
                        Payload=json.dumps(payload)
                    )

                    response_payload = json.loads(response['Payload'].read())
                    logger.info(f'Lambda response: {response_payload}')
                    return response_payload

                except ClientError as e:
                    logger.error(f"Error invoking Lambda function: {e}")
                    raise

        else:
            logger.info("No objects found in the specified S3 path.")

    except Exception as e:
        logger.error(f"Error: {e}")
        raise

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