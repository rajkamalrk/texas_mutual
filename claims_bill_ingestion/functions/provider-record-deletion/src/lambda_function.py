import base64
import json
import os
import logging
import pymysql
import boto3
from botocore.exceptions import ClientError
from datetime import datetime

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_secret(secret_name: str, region_name: str) -> dict:
    """
    Retrieve secret from AWS Secrets Manager and return as dict.
    """
    session = boto3.session.Session()
    client = session.client("secretsmanager", region_name=region_name)
    try:
        response = client.get_secret_value(SecretId=secret_name)
        secret_str = response.get("SecretString")
        if secret_str:
            return json.loads(secret_str)
        secret_bin = response.get("SecretBinary")
        if secret_bin:
            # Decode bytes to string before loading JSON
            return json.loads(base64.b64decode(secret_bin).decode("utf-8"))
        logger.error("No SecretString or SecretBinary found in secret response.")
        raise ValueError("Invalid secrets manager value")
    except ClientError as e:
        logger.error("Unable to retrieve secret: %s", e)
        raise
    except Exception as e:
        logger.error("Parsing secret failed: %s", e)
        raise


def update_db_records(
    secrets: dict, update_db_query: str, db_name, provider_number: str
) -> int:
    """
    Update the provider row, return number of affected rows.
    """
    logger.info("Query str: %s", update_db_query)
    update_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    connection = None
    try:
        connection = pymysql.connect(
            host=secrets["host"],
            user=secrets["username"],
            password=secrets["password"],
            port=int(secrets["port"]),
            database=db_name,  # <--- FIX: Use the correct variable
            connect_timeout=300,
            cursorclass=pymysql.cursors.Cursor,
            autocommit=True,
        )
        with connection.cursor() as cursor:
            cursor.execute(update_db_query, (update_time, provider_number))
            affected_rows = cursor.rowcount
        connection.commit()
        return affected_rows
    except KeyError as e:
        logger.error("Secret missing key: %s", e)
        return 0
    except Exception as e:
        logger.error("Error updating database: %s", e)
        if connection:
            connection.rollback()
        return 0
    finally:  # <-- Ensure connection is always closed!
        if connection:
            connection.close()
            logger.info("Database connection closed")


def lambda_handler(event, context):
    logger.info("Received event: %s", json.dumps(event))
    try:
        provider_number = None
        path_params = event.get("pathParameters")
        if path_params and path_params.get("provider_number"):
            provider_number = path_params["provider_number"]
        if not provider_number:
            logger.warning("provider_number missing in pathParameters")
            return {
                "statusCode": 400,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps(
                    {"message": "Invalid request, provider number required"}
                ),
            }
        secret_name = os.environ.get("SECRET_NAME")
        region = os.environ.get("REGION")
        table_name = os.environ.get("TABLE_2")
        db_name = os.environ.get("DB_2")
        logger.info("Table name: %s", table_name)
        logger.info("DB name: %s", db_name)
        if not secret_name or not region:
            logger.error("Environment variables SECRET_NAME or REGION are not set")
            return {
                "statusCode": 500,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps(
                    {"message": "Configuration missing", "status": "error"}
                ),
            }
        secrets = get_secret(secret_name, region)
        update_db_sql = (
            f"UPDATE {db_name}.{table_name} "
            f"SET vendor_status = 'Inactive', "
            f"delivered_to_sa = 'NEW', "
            f"update_user = 'LAMBDA API', "
            f"update_timestamp = %s"
            f"WHERE provider_number = %s"
        )
        logger.info("Query is: %s", update_db_sql)
        affected = update_db_records(secrets, update_db_sql, db_name, provider_number)
        if affected > 0:
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps(
                    {
                        "message": f"Provider number '{provider_number}' has been updated successfully",
                        "status": "success",
                    }
                ),
            }
        else:
            logger.info("No provider_numbers updated")
            return {
                "statusCode": 404,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"message": "No provider_numbers found or updated"}),
            }
    except Exception as e:
        logger.exception("Lambda handler error: %s", e)
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"status": "Internal server error"}),
        }
