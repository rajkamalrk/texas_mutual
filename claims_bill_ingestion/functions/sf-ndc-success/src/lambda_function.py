import os
import logging
import json
import dateutil.tz
import base64
import boto3
from botocore.exceptions import ClientError
import pymysql
from datetime import datetime


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

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


def log_and_email(env_profile,provider,reprocess_flag,batch_id,data_pipeline_name,data_pipeline_id,error_info,error_message,severity,sns_topic_arn):
    """logging error and sending error email"""
    print("Inside log email method")
    sns_client=boto3.client('sns',region_name='us-east-1')
    cst=dateutil.tz.gettz('America/Chicago')
    timestamp=datetime.now(cst).strftime('%Y/%m/%d %H:%M:%S %Z%z')
    provider = provider.upper()
    #create log message
    process=f"Process : {provider} - Failure Flow Lambda"
    errInfo=f"Error  : {error_info}"
    reprocessflag=f"Reprocess Flag  : {reprocess_flag}"
    batchid=f"Step Function Batch ID: {batch_id}"
    stepFuncName=f"Step Function Name: {data_pipeline_name}"
    stepFuncId=f"Step Function ID: {data_pipeline_id}"
    time=f"Timestamp : {timestamp}"
    errLog=f"Fields & Content from Error log : {error_message}"
    message_body=f"{process}\n{errInfo}\n{reprocessflag}\n{batchid}\n{stepFuncName}\n{stepFuncId}\n{time}\n{errLog}"
    print(message_body)
    subject=f"{env_profile} - {provider} - {error_info} -{severity}"
    
    response=sns_client.publish(
       TopicArn=sns_topic_arn,
       Subject=subject,
       Message=message_body
       )
    return response


def lambda_handler(event, context):
    print("Processing STARTED...")
    print("event", event)
    
    status = "SUCCEED"
    status_code = 200
    batch_id = ""
    provider = ""
    data_pipeline_name = ""
    data_pipeline_id = ""
    reprocess_flag= ""

    env_profile = os.environ['env_profile']
    print(f'env_profile => {env_profile}')
    db_secret_key = os.environ['db_secret_key']
    print(f"db_secret_key => {db_secret_key}")
    sns_topic_arn = os.environ['sns_topic_arn']
    print(f'sns_topic_arn => {sns_topic_arn}')
    aws_request_id = context.aws_request_id
    print(f"Lambda Request ID: {aws_request_id}")
    lambda_name = context.function_name
    print(f"Lambda Function Name: {lambda_name}")
    step_function_arn = os.environ['step_function_arn']
    print(f'step_function_arn => {step_function_arn}')

    try:
        status = "COMPLETED"
        batch_id= ""

        if isinstance(event, dict):
            secrets = get_secret(db_secret_key, 'us-east-1')
            print(f'event=>{event}')
            # Get the payload
            if 'parsedBody' in event:
                payload=event['parsedBody']
                
                batch_id = payload.get('batch_id')
                print(f'batch_id=>{batch_id}')
                provider = payload.get('provider')
                print(f'provider=>{provider}')
                reprocess_flag = payload.get('reprocess_flag')
                print(f'reprocess_flag=>{reprocess_flag}')
                data_pipeline_name = payload.get('step_function_info')
                print(f'data_pipeline_name=>{data_pipeline_name}')
                data_pipeline_id = payload.get('step_function_execution_id')
                print(f'data_pipeline_id=>{data_pipeline_id}')     
                  
            v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            end_datetime = v_load_timestamp
            
            # Update batch status to 'FAILED'
            update_sql = f"""
                UPDATE txm_ndc_logs.batch_ndc_master
                SET status = 'COMPLETED',
                    end_datetime= '{v_load_timestamp}',
                    update_user = 'Lambda - Success Flow',
                    update_timestamp = '{end_datetime}'
                WHERE batch_id = '{batch_id}'
            """
            print(f"[FAILED] update_sql => {update_sql}")
            run_sql_query(update_sql, secrets)
            print(f"[FAILED] Successfully updated batch_status_id {batch_id} to FAILED.")


            result = {
                'batch_id': batch_id,
                'status': status,
                'Comment': "All Process Completed Sucessfully ....",
            }
            print(f"result => {result}")

        else:
            print("Unexpected event format:", event)
            raise Exception("Unexpected event format:")
            
    except Exception as e:
            print(f"Error decoding JSON: {str(e)}")
            error_message=str(e)[:500]
            #Trigger Error notification
            severity="FATAL"
            error_info_subj = f"Failed in execution of Step Function - {data_pipeline_name}"
            response=log_and_email(env_profile,provider,reprocess_flag,batch_id,data_pipeline_name,data_pipeline_id,error_info_subj,json.dumps(error_message),severity,sns_topic_arn)
            print(f"Email sent successfully!message_id:{response['MessageId']}")

    return {
        'statusCode': status_code,
        'body': result
    }
