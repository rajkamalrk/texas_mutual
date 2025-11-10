import os
import logging
import json
import base64
import boto3
from botocore.exceptions import ClientError
import pymysql
from datetime import datetime

# Configure Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

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
    output = None
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
            if db_sql.strip().upper().startswith("SELECT") or db_sql.strip().upper().startswith("WITH"):
                output = cursor.fetchall()  # Return results for SELECT queries
            connection.commit()
            print('Query executed successfully')
            return output
    finally:
        connection.close()

def lambda_handler(event, context):
    print("Received event: %s", json.dumps(event))
    
    # List to hold extracted values from the messages
    result = dict()
    status_code = 200
    file_header_id = ""
    source = ""
    trading_partner = ""
    reprocess_flag = ""
    aws_request_id = context.aws_request_id
    print(f"Lambda Request ID: {aws_request_id}")
    lambda_name = context.function_name
    print(f"Lambda Function Name: {lambda_name}")

    try:
        if isinstance(event, dict):
            print("Starting to process record...")
        
            db_secret_key = os.environ['db_secret_key']
            print(f"Retrieved db_secret_key: {db_secret_key}")
            
            secrets = get_secret(db_secret_key, 'us-east-1')

            # Get the payload
            result = event['parsedBody']
            print(f"Payload retrieved: {result}")

            # Extract required fields
            file_header_id = result.get('file_header_id')
            source = result.get('source')
            trading_partner = result.get('trading_partner')
            reprocess_flag = result.get('reprocess_flag')
            print(f"Extracted fields - file_header_id: {file_header_id}, source: {source}, trading_partner: {trading_partner}, reprocess_flag: {reprocess_flag}")
        
            if reprocess_flag == "Y" and file_header_id:
                print(f"File Header ID exists file_header_id: {file_header_id}, Skipping rest of the reprocess flaow...")
            else:
                # Find records with null header_id for each form type
                # Mapping of query types to their respective SQL details
                if source == 'kofax':
                    query_map = {
                        "cms": ("cms_id", "txm_bitx_kofax_staging.kofax_cms1500"),
                        "dwc": ("dwc_id", "txm_bitx_kofax_staging.kofax_dwc066"),
                        "ub": ("ub_id", "txm_bitx_kofax_staging.kofax_ub04")
                    }
                elif source == 'eci':
                    query_map = {
                        "eci": ("bill_id", "txm_bitx_eci_staging.eci_bills")
                    }
                else:
                    error_message = f"Invalid input Source {source}!!!"
                    print(f"error_message => {error_message}")
                    raise Exception(error_message)

                # Dictionary to store results
                all_results = {}

                # Iterate over the query map
                for query_type, (column, table) in query_map.items():
                    # Constructing the SQL query dynamically
                    sql_query = f"SELECT COUNT({column}) AS record_count FROM {table} f WHERE f.header_id IS NULL"
                    print(f'sql_query=>{sql_query}')
                    # Execute the constructed query
                    results = run_sql_query(sql_query, secrets)
                    print(results)
                    results=results[0][0]
                    print(results)
                
                    # Store results in the dictionary
                    all_results[query_type] = results
                    print(f"Results for {query_type.upper()}: {results}")

                # Print the results
                print(f"all_results => {all_results}")
            
                v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                print(v_load_timestamp)
                # Load empty record in header table with some value like etl fields so that file header generated
                # Check if there are any records with NULL header_id in any of the tables
                count = max(all_results.values())
                print(f'all_results.values()=>{all_results.values()}')
                print(f'count=>{count}')
                if count>0:
                    # Load empty record in kofax header table with some value like ETL fields
                    if source == 'kofax':
                        header_table_name = "txm_bitx_kofax_staging.kofax_header"
                    elif source == 'eci':
                        header_table_name = "txm_bitx_eci_staging.eci_bills_header"
                    insert_query = f"""
                    INSERT INTO {header_table_name} (create_user, update_user, create_timestamp, update_timestamp)
                    VALUES ('Glue ETL - Non-ebill Enhanced load', 'Glue ETL - Non-ebill Enhanced load', '{v_load_timestamp}', '{v_load_timestamp}');
                    """
                    print(f'insert_query=>{insert_query}')
                    run_sql_query(insert_query, secrets)
                
                    # Extracting file_header_id from header_table after loading rec in header staging table
                    header_read_db_sql=f"""SELECT header_id FROM {header_table_name} WHERE create_timestamp = '{v_load_timestamp}'"""
                    print(f'header_read_db_sql =>{header_read_db_sql}')
                    file_header_id_result=run_sql_query(header_read_db_sql, secrets)
                    print(f'Sucessfully executed =>{header_read_db_sql}')
                    file_header_id = file_header_id_result[0][0]
                    print(f'Generated new file_header_id =>{file_header_id}')

                    result["file_header_id"] = file_header_id

                    # Assign that header_id to records found with NULL header_id
                    print(f"all_results=> {all_results}")
                    for query_type, (column, table) in query_map.items():
                        # Construct update query to assign the new header_id
                        update_query = f"""
                        UPDATE {table}
                        SET header_id = {file_header_id},
                        update_user = 'Glue ETL - Non-ebill Enhance loading',
                        update_timestamp = '{v_load_timestamp}'
                        WHERE header_id IS NULL
                        """
                        print(f'update_query for {query_type} => {update_query}')
                        
                        # Execute the update query
                        run_sql_query(update_query, secrets)
                        print(f"Updated table {table} with header_id: {file_header_id}")

                else:
                    print(f"No Data Available for Source {source}, trading partner {trading_partner} and File Header ID {file_header_id}. Skipping the rest of the flow!!!")
                    error_message=f"No Data Found in the Batch. Stopping further batch processing!!!"
                    print(error_message)
                    raise Exception(error_message)

            print(f"result => {result}")
            print("Finished processing record.")
        else:
            error_message=f"Unexpected event format: {event}"
            print(error_message)
            raise Exception(error_message)
            
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        status_code = 500
        result["file_header_id"] = file_header_id
        result["errorMessage"] = str(e)
    return {
        'statusCode': status_code,
        'requestId': aws_request_id,
        "lambdaName": lambda_name,
        'body': json.dumps(result)
    }