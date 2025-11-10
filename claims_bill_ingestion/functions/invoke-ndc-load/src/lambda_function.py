import json
import boto3
import os
import dateutil.tz
import base64
from datetime import datetime
from botocore.exceptions import ClientError
import time
import pymysql

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
        print("Error retrieving secret '%s': %s", secret_name, str(e))
        raise e
    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response.get('SecretString', '')
    if not secret: 
        decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
        secret = decoded_binary_secret
    return json.loads(secret)

def delete_s3_file(bucket_name, key):
    """Delete an S3 file"""
    try:
        s3_resource = boto3.resource('s3')
        s3_resource.Object(bucket_name, key).delete()
    except Exception as e:
        print("Error deleting file: %s", str(e))
        raise e

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
            print(f"S3 response doesn't have Key Contents. response => {response}")
    if s3_files:
        print(f"List of files available for processing with prefix => {prefix} and suffix => {suffix}")
        print(f"s3_files => {s3_files}")
    else:
        print(f"No files available for processing with prefix => {prefix} and suffix => {suffix}")
    return s3_files
    
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
            if db_sql.strip().upper().startswith("SELECT"):
                return cursor.fetchall()  # Return results for SELECT queries
            connection.commit()
    finally:
        connection.close()


def log_and_email(env_profile,provider,file_name,reprocess_flag,batch_id,error_info,error_message,severity,sns_topic_arn):
    """logging error and sending error email"""
    print("Inside log email method")
    sns_client=boto3.client('sns',region_name='us-east-1')
    cst=dateutil.tz.gettz('America/Chicago')
    timestamp=datetime.now(cst).strftime('%Y/%m/%d %H:%M:%S %Z%z')
    provider = provider.upper()
    #create log message
    process=f"Process : {provider} - Invoke NDC Load Step Function"
    errInfo=f"Error  : {error_info}"
    fileid=f"File Name (.completed): {file_name}"
    reprocessflag=f"Reprocess Flag  : {reprocess_flag}"
    batchid=f"Step Function Batch ID: {batch_id}"
    time=f"Timestamp : {timestamp}"
    errLog=f"Fields & Content from Error log : {error_message}"
    message_body=f"{process}\n{errInfo}\n{fileid}\n{reprocessflag}\n{batchid}\n{time}\n{errLog}"
    print(message_body)
    subject=f"{env_profile} - {provider} - {error_info} -{severity}"
    
    response=sns_client.publish(
       TopicArn=sns_topic_arn,
       Subject=subject,
       Message=message_body
       )
    return response

def get_ndc_batch_id(provider,secrets):
    v_start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    print(f"v_start_time => {v_start_time}")
    v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    create_timestamp= v_load_timestamp
    update_timestamp= v_load_timestamp
    print(f"v_load_timestamp => {v_load_timestamp}")
    insert_sql = f"""
            INSERT INTO txm_ndc_logs.batch_ndc_master (
                provider, start_datetime, status, create_user, create_timestamp, update_user, update_timestamp) 
                VALUES ('{provider}', '{v_start_time}', 'IN PROGRESS', 'Lambda - Invoke NDC', '{create_timestamp}','Lambda - Invoke NDC', '{update_timestamp}')
            """
    print(f'insert_sql={insert_sql}') 
    run_sql_query(insert_sql, secrets)
    print(f"Sucesfully inserted log data in txm_ndc_logs.batch_ndc_master table....")

    # Retrieving batch_status_id 
    run_db_sql = f"""SELECT batch_id FROM txm_ndc_logs.batch_ndc_master WHERE create_timestamp = '{create_timestamp}' AND start_datetime = '{v_start_time}'"""
    print(f"run_db_sql=>{run_db_sql}")
    batch_status_df=run_sql_query(run_db_sql, secrets)
    batch_status_id=batch_status_df[0][0]
    print(f'Successfully fetched batch_status_id:{batch_status_id}')
    return batch_status_id


def lambda_handler(event, context):
    try:
        print("Event => ", event)

        payload = dict()
        provider = ""
        bill_file = ""
        bill_file_ext = ""
        bill_file_details = dict()
        reprocess_flag = "N"
        batch_id = ""
        file_name = ""
        env_profile = os.environ['env_profile']
        print(f'env_profile => {env_profile}')
        sqs_queue_url = os.environ['sqs_queue_url']
        print(f'sqs_queue_url => {sqs_queue_url}')
        step_function_arn = os.environ['step_function_arn']
        print(f'step_function_arn => {step_function_arn}')
        sns_topic_arn = os.environ['sns_topic_arn']
        print(f'sns_topic_arn => {sns_topic_arn}')
        db_secret_key = os.environ['db_secret_key']
        print(f"db_secret_key => {db_secret_key}")
        aws_request_id = context.aws_request_id
        print(f"Lambda Request ID: {aws_request_id}")
        lambda_name = context.function_name
        print(f"Lambda Function Name: {lambda_name}")

        secrets = get_secret(db_secret_key, 'us-east-1')
        print(f'secrets=>{secrets}')
        
        event_records = []
        # Check if 'Records' is inside a list
        if isinstance(event, list):
            for record in event:
                event_records.append(record)
        elif isinstance(event, dict) and 'Records' in event:
            for record in event['Records']:
                event_records.append(record)
        else:
            error_message = f"Unexpected event format: {event}"
            print(f"error_message => {error_message}")
            raise Exception(error_message)

        for record in event_records:
            # Get the message body
            message_body = record['body']
            
            # Process the message (this is where you would add your custom processing logic)
            print(f"Message Body: {message_body}")

            message_body_json = json.loads(message_body)
            print(f"Parsed Message: {message_body_json}")

            messages = []
            # Check if 'Records' is inside a list
            if isinstance(message_body_json, list):
                for message in message_body_json:
                    messages.append(message)
            elif isinstance(message_body_json, dict) and 'Records' in message_body_json:
                for message in message_body_json['Records']:
                    messages.append(message)
            else:
                error_message = f"Unexpected message body format: {message_body_json}"
                print(f"error_message => {error_message}")
                raise Exception(error_message)
            
            for message in messages:
                # Extract required fields
                s3_bucket = message['s3']['bucket']['name']
                s3_object_key = message['s3']['object']['key']
                file_create_date_time=message['eventTime']
                file_create_date=datetime.strptime(file_create_date_time, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d")
                print(f"s3_bucket => {s3_bucket}")
                print(f"s3_object_key => {s3_object_key}")
                print(f"file_create_date => {file_create_date}")

                folder_path_list = s3_object_key.split('/')
                print(f"folder_path_list => {folder_path_list}")
                provider = folder_path_list[-2].lower()
                print(f"provider => {provider}")
                file_name = folder_path_list[-1]
                print(f"file_name => {file_name}")
                file_name_wo_ext = file_name.rsplit('.', 1)[0]
                print(f"file_name_wo_ext => {file_name_wo_ext}")
                file_ext = file_name.split('.')[-1]
                print(f"file_ext => {file_ext}")
                #get the folder name
                folder_prefix = s3_object_key.replace(file_name,"")
                print(f"folder_prefix => {folder_prefix}")

                batch_id = get_ndc_batch_id(provider,secrets)
                print(f"batch_id => {batch_id}")


                payload = {
                    'provider' : provider,
                    's3_bucket': s3_bucket,
                    'reprocess_flag': 'N',
                    'batch_id': batch_id
                }


                print(f"payload => {payload}")

                sf_client = boto3.client('stepfunctions')
                invoke_response = sf_client.start_execution(
                    stateMachineArn=step_function_arn,
                    input=json.dumps(payload)
                )
                print(f'invoke_response => {invoke_response}')

                #updated log table
                v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                create_timestamp= v_load_timestamp
                update_timestamp= v_load_timestamp
                update_sql = f"""
                UPDATE txm_ndc_logs.batch_ndc_master
                SET file_name = '{file_name}',file_create_date = '{file_create_date}',file_path = '{folder_prefix}',data_pipeline_id='{invoke_response['executionArn']}', update_user = 'Lambda - Invoke NDC', update_timestamp = '{update_timestamp}'
                WHERE batch_id = {batch_id}
                """
                print(f"[SUCCESS] update_sql => {update_sql}")
                run_sql_query(update_sql, secrets)
                print(f"[SUCCESS] Successfully updated batch_status_id {batch_id} to SUCCESS.")

                print(f"Deleting .completed file {s3_object_key}...")
                delete_s3_file(bucket_name=s3_bucket, key=s3_object_key)

                if len(messages) > 1:
                    # Delaying multiple batches together to avoid conflicts.
                    print("Delaying multiple batches together to avoid conflicts...Sleep for 15 seconds")
                    time.sleep(15)
            
            # Optionally, delete the message from the queue if processing is successful
            receipt_handle = record['receiptHandle'] 
            print(f"receipt_handle => {receipt_handle}")
            sqs_client = boto3.client('sqs')
            delete_response = sqs_client.delete_message(QueueUrl= sqs_queue_url,ReceiptHandle=receipt_handle)
            print(f'delete_response => {delete_response}')

            if len(event_records) > 1:
                # Delaying multiple batches together to avoid conflicts.
                print("Delaying multiple batches together to avoid conflicts...Sleep for 15 seconds")
                time.sleep(15)
        
    except json.JSONDecodeError as e:
        print(f"Message is not in JSON format. {str(e)}")
        raise e
    except Exception as e:
        print(f"Exception Block Error dump: {str(e)}")
        severity="FATAL"
        errorInfo="Unable to invoke Staging Load Step Function"
        errorMessage=str(e)[:500]
        response=log_and_email(env_profile,provider,file_name,reprocess_flag,batch_id,errorInfo,errorMessage,severity,sns_topic_arn)
        print(f"Email sent successfully!message_id:{response['MessageId']}")
        if batch_id:
            #updated log table
            v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            create_timestamp= v_load_timestamp
            update_timestamp= v_load_timestamp
            update_sql = f"""
            UPDATE txm_ndc_logs.batch_ndc_master
            SET status = 'FAILED', update_user = 'Lambda - Invoke NDC', update_timestamp = '{update_timestamp}'
            WHERE batch_id = '{batch_id}'
            """
            print(f"[FAILED] update_sql => {update_sql}")
            run_sql_query(update_sql, secrets)
            print(f"[FAILED] Successfully updated batch_status_id {batch_id} to FAILED.")

            insert_error_sql = f"""
            INSERT INTO txm_ndc_logs.batch_ndc_error_details (
                batch_status_id,error_code,error_info, error_message, create_user, create_timestamp,update_user,update_timestamp
            ) VALUES ({batch_id},'INVOKE NDC LOAD',{errorInfo}', '{errorMessage}', 'Lambda - Invoke NDC', '{create_timestamp}', 
                'Lambda - Invoke NDC', '{update_timestamp}'
            )
            """
            print(f"[FAILURE] insert_error_sql => {insert_error_sql}")
            run_sql_query(insert_error_sql, secrets)
            print(f"[FAILURE] Logged failure for batch_status_id {batch_id}")
        raise