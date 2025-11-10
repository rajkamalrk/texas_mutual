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
            connection.commit()
    finally:
        connection.close()

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

def log_and_email(env_profile,source,trading_partner,file_name,reprocess_flag,sf_batch_id,error_info,error_message,severity,sns_topic_arn):
    """logging error and sending error email"""
    print("Inside log email method")
    sns_client=boto3.client('sns',region_name='us-east-1')
    cst=dateutil.tz.gettz('America/Chicago')
    timestamp=datetime.now(cst).strftime('%Y/%m/%d %H:%M:%S %Z%z')
    if trading_partner:
       source_tp_str = trading_partner
    else:
       source_tp_str = source
    source_tp_str = source_tp_str.upper()
    #create log message
    process=f"Process : {source_tp_str} - Invoke Staging Load Step Function"
    errInfo=f"Error  : {error_info}"
    fileid=f"File Name (.completed): {file_name}"
    reprocessflag=f"Reprocess Flag  : {reprocess_flag}"
    batchid=f"Step Function Batch ID: {sf_batch_id}"
    time=f"Timestamp : {timestamp}"
    errLog=f"Fields & Content from Error log : {error_message}"
    message_body=f"{process}\n{errInfo}\n{fileid}\n{reprocessflag}\n{batchid}\n{time}\n{errLog}"
    print(message_body)
    subject=f"{env_profile} - {source_tp_str} - {error_info} -{severity}"
    
    response=sns_client.publish(
       TopicArn=sns_topic_arn,
       Subject=subject,
       Message=message_body
       )
    return response

def lambda_handler(event, context):
    try:
        print("Event => ", event)

        payload = dict()
        source = "ebill"
        trading_partner = ""
        bill_file = ""
        bill_file_ext = ""
        image_file = ""
        image_file_ext = ""
        bill_file_details = dict()
        image_file_details = None
        reprocess_flag = "N"
        sf_batch_id = ""
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
                print(f"s3_bucket => {s3_bucket}")
                print(f"s3_object_key => {s3_object_key}")

                folder_path_list = s3_object_key.split('/')
                print(f"folder_path_list => {folder_path_list}")
                trading_partner = folder_path_list[-2].lower()
                print(f"trading_partner => {trading_partner}")
                file_name = folder_path_list[-1]
                print(f"file_name => {file_name}")
                file_name_wo_ext = file_name.split('.')[0]
                print(f"file_name_wo_ext => {file_name_wo_ext}")
                file_ext = file_name.split('.')[-1]
                print(f"file_ext => {file_ext}")
                #get the folder name
                folder_prefix = s3_object_key.replace(file_name,"")
                print(f"folder_prefix => {folder_prefix}")

                input_payload = {"s3_bucket": s3_bucket, "s3_object_key": s3_object_key}
                db_sp_params = source, trading_partner, None, None, None, json.dumps(input_payload), None, None, None ,None, None, None, 'INVOKE STAGING', reprocess_flag,  'Lambda - Invoke Staging', 'Lambda - Invoke Staging', 0
                print(f"db_sp_params => {db_sp_params}")
                dbStoredProcedure = "txm_bitx_logs.insert_step_function_logs"
                print(f"dbStoredProcedure => {dbStoredProcedure}")
                sf_batch_id = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, 16)
                print(f"sf_batch_id => {sf_batch_id}")

                res_s3_bill_file_list = get_s3_objects(bucket = s3_bucket, prefix = folder_prefix + file_name_wo_ext, suffix = None)
                print(f"res_s3_bill_file_list => {res_s3_bill_file_list}")

                s3_bill_file_list = [i for i in res_s3_bill_file_list if i != s3_object_key]
                print(f"s3_bill_file_list => {s3_bill_file_list}")

                if s3_bill_file_list:
                    bill_file = s3_bill_file_list[0]
                    bill_file_ext = bill_file.split('.')[-1]

                    bill_file_details = {
                        "file_name": bill_file,
                        "file_extention": bill_file_ext
                    }

                    if trading_partner == "jopari":
                        res_s3_image_file_list = get_s3_objects(bucket = s3_bucket, prefix = folder_prefix + file_name_wo_ext.replace('Bills','Images'), suffix = None)
                        print(f"res_s3_image_file_list => {res_s3_image_file_list}")

                        s3_image_file_list = [i for i in res_s3_image_file_list if i != s3_object_key]
                        print(f"s3_image_file_list => {s3_image_file_list}")

                        if s3_image_file_list:
                            image_file = s3_image_file_list[0]
                            image_file_ext = image_file.split('.')[-1]

                            image_file_details = {
                                "file_name": image_file,
                                "file_extention": image_file_ext
                            }
                else:
                    error_message = f"No bill file present in s3 bucket: {s3_bucket} and prefix: {folder_prefix + file_name_wo_ext}"
                    print(f"error_message => {error_message}")
                    raise Exception(error_message)
                
                print(f"bill_file => {bill_file}")
                print(f"bill_file_ext => {bill_file_ext}")
                print(f"bill_file_details => {bill_file_details}")
                print(f"image_file => {image_file}")
                print(f"image_file_ext => {image_file_ext}")
                print(f"image_file_details => {image_file_details}")

                payload = {
                    'source' : source,
                    'trading_partner': trading_partner,
                    's3_bucket': s3_bucket,
                    'bill_file': bill_file_details,
                    'image_file': image_file_details,
                    'reprocess_flag': reprocess_flag,
                    'sf_batch_id': sf_batch_id
                }

                print(f"payload => {payload}")

                sf_client = boto3.client('stepfunctions')
                invoke_response = sf_client.start_execution(
                    stateMachineArn=step_function_arn,
                    input=json.dumps(payload)
                )
                print(f'invoke_response => {invoke_response}')

                #updated log table
                db_sp_params = sf_batch_id, None, bill_file, image_file, None, invoke_response['executionArn'], json.dumps(payload), None ,None, None, None, 'STAGING', None,  'Lambda - Invoke Staging'
                print(f"db_sp_params => {db_sp_params}")
                dbStoredProcedure = "txm_bitx_logs.update_step_function_log_status"
                print(f"dbStoredProcedure => {dbStoredProcedure}")
                res = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, None)
                print(f"res => {res}")

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
        response=log_and_email(env_profile,source,trading_partner,file_name,reprocess_flag,sf_batch_id,errorInfo,errorMessage,severity,sns_topic_arn)
        print(f"Email sent successfully!message_id:{response['MessageId']}")
        if sf_batch_id:
            #updated log table
            db_sp_params = sf_batch_id, 'LAMBDA', lambda_name, aws_request_id, json.dumps(payload), 'FAILED', 'INVOKE STAGING LOAD', errorInfo, errorMessage, 'Lambda - Invoke Staging', 'Lambda - Invoke Staging'
            print(f"db_sp_params => {db_sp_params}")
            dbStoredProcedure = "txm_bitx_logs.update_step_function_failure_log_status"
            print(f"dbStoredProcedure => {dbStoredProcedure}")
            res = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, None)
            print(f"res => {res}")
        raise