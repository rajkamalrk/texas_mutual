import boto3
import json
import logging
import os
 
# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
 
# Initialize the Secrets Manager client
secrets_manager_client = boto3.client('secretsmanager')
 
def lambda_handler(event, context):
    try:
        # Replace these values with your secret ARNs or names
        source_secret_name = os.environ['rds_secret']
        target_secret_name = os.environ['appuser_secret']
        try:
            target_secret_names = json.loads(target_secret_name)
            if not isinstance(target_secret_names, list):
                raise ValueError("Parsed JSON is not a list.")
        except json.JSONDecodeError:
            # If not JSON, split by comma
            target_secret_names = [s.strip() for s in target_secret_name.split(',') if s.strip()]

        logger.info(f"Target secrets to update: {target_secret_names}")
 
        # Retrieve the source secret value
        logger.info(f"Retrieving source secret: {source_secret_name}")
        source_secret_response = secrets_manager_client.get_secret_value(SecretId=source_secret_name)
        source_secret_value = json.loads(source_secret_response['SecretString'])
        logger.info("Source secret retrieved successfully.")
 
        # Retrieve the target secret value
        logger.info(f"Retrieving target secret: {target_secret_name}")
        target_secret_response = secrets_manager_client.get_secret_value(SecretId=target_secret_name)
        target_secret_value = json.loads(target_secret_response['SecretString'])
        logger.info("Target secret retrieved successfully.")
 
        # Update only the password in the target secret
        target_secret_value['password'] = source_secret_value['password']
 
        # Update the target secret with the modified value
        logger.info(f"Updating target secret: {target_secret_name}")
        
        update_secret_response = secrets_manager_client.update_secret(
            SecretId=target_secret_name,
            SecretString=json.dumps(target_secret_value)
        )
        logger.info("Target secret updated successfully.")
        logger.info(f"Update secret response: {update_secret_response}")
        return {
            "statusCode": 200,
            "body": json.dumps("Password successfully updated!")
        }
        
 
    except Exception as e:
        logger.error(f"Error occurred: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps(f"Error: {str(e)}")
        }
 