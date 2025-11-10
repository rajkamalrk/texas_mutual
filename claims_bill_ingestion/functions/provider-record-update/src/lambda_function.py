import base64
import json
import logging
import os
import pymysql
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Required GLOBALS for schema/table; set as needed
DB_NAME = "txm_bitx_provider"
TABLE_NAME = "provider"


def get_secret(secret_name, region_name):
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        if "SecretString" in get_secret_value_response:
            return json.loads(get_secret_value_response["SecretString"])
        elif "SecretBinary" in get_secret_value_response:
            secret = get_secret_value_response["SecretBinary"]
            # Decode if it's bytes
            if isinstance(secret, bytes):
                secret = secret.decode("utf-8")
            return json.loads(base64.b64decode(secret))
        else:
            raise Exception(
                "No SecretString or SecretBinary found in SecretManager response"
            )
    except ClientError as e:
        logger.error("Error retrieving secret '%s': %s", secret_name, str(e))
        raise


def get_db_connection():
    secrets = get_secret(os.environ["SECRET_NAME"], os.environ["REGION"])
    dbname = (
        secrets.get("dbname")
        or secrets.get("database")
        or secrets.get("dbClusterIdentifier")
        or DB_NAME  # Fallback to default global if missing
    )
    connection = pymysql.connect(
        host=secrets["host"],
        db=dbname,  # FIXED: use 'db' not 'database'
        port=int(secrets["port"]),
        user=secrets["username"],
        password=secrets["password"],
        connect_timeout=300,
        cursorclass=pymysql.cursors.DictCursor,
    )
    return connection


FIELD_MAP = {
    "billItemType": "bill_item_type",
    "renderingProviderLicense": "rendering_provider_license",
    "renderingProviderName": "rendering_provider_name",
    "renderingProviderNpi": "rendering_provider_npi",
    "facilityName": "facility_name",
    "facilityAddress1": "facility_address_1",
    "facilityAddress2": "facility_address_2",
    "facilityCity": "facility_city",
    "facilityState": "facility_state",
    "facilityZipCode": "facility_zip_code",
    "facilityNpi": "facility_npi",
}


def update_provider(
    cursor, data, provider_number, db_name=DB_NAME, table_name=TABLE_NAME
):
    set_clauses = []
    update_values = {}
    for req_json_field, db_field in FIELD_MAP.items():
        val = data.get(req_json_field, None)
        if val is not None and val != "":
            set_clauses.append(f"{db_field} = %({db_field})s")
            update_values[db_field] = val

    if not set_clauses:
        # Instead of raising, caller will handle as 400
        return None

    set_clause_str = ", ".join(
        set_clauses
        + [
            "delivered_to_sa = 'NEW'",
            "update_timestamp = NOW()",
            "provider_type_code = NULL",
        ]
    )
    update_sql = f"""
        UPDATE {db_name}.{table_name}
        SET
            {set_clause_str}
        WHERE provider_number = %(provider_number)s
    """
    update_values["provider_number"] = provider_number
    cursor.execute(update_sql, update_values)
    select_sql = f"""
        SELECT bill_item_type, rendering_provider_license, rendering_provider_name, rendering_provider_npi,
               facility_name, facility_address_1, facility_address_2, facility_city, vendor_number, vendor_name, 
               vendor_tax_id, vendor_tax_id_indicator, vendor_license_type, vendor_address1, vendor_address2, vendor_city,
               vendor_state, vendor_zip_code, vendor_phone, vendor_status, facility_state, facility_zip_code, facility_npi,
               rendering_provider_jurisdiction, provider_type_code, delivered_to_sa, update_timestamp
        FROM {db_name}.{table_name}
        WHERE provider_number = %s
    """
    cursor.execute(select_sql, (provider_number,))
    updated_data = cursor.fetchone()
    logger.info("Provider updated successfully: %s", updated_data)
    return updated_data


def lambda_handler(event, context):
    logger.info("Received event: %s", event)
    try:
        # ------ MODIFIED block ------
        # Validate provider_number in pathParameters
        provider_number = None
        if (
            "pathParameters" not in event
            or event["pathParameters"] is None
            or "provider_number" not in event["pathParameters"]
            or not event["pathParameters"]["provider_number"]
        ):
            return {
                "statusCode": 404,
                "body": json.dumps(
                    {
                        "status": "error",
                        "message": "provider_number not found",
                    },
                ),
            }
        provider_number = event["pathParameters"]["provider_number"]
        # ------ END MODIFIED block ------

        body = json.loads(event.get("body", "{}"))
        data = {}
        for json_field, db_field in FIELD_MAP.items():
            if json_field in body and body[json_field] not in ("", None):
                data[json_field] = body[json_field]

        if not data:
            return {
                "statusCode": 404,  # 404 is "not found", 400 is "bad request"
                "body": json.dumps(
                    {
                        "status": "error",
                        "message": "No updatable fields provided",
                    }
                ),
            }
        connection = get_db_connection()
        try:
            with connection.cursor() as cursor:
                updated_data = update_provider(cursor, data, provider_number)
                if updated_data is None:
                    return {
                        "statusCode": 400,
                        "body": json.dumps(
                            {
                                "status": "error",
                                "message": "provider_number not found",
                            }
                        ),
                    }
                connection.commit()
        finally:
            connection.close()
        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "status": "Success",
                    "message": f"Provider '{provider_number}' updated successfully",
                    "updated_data": updated_data,
                },
                default=str,
            ),
        }
    except json.JSONDecodeError as e:
        logger.error("Error parsing JSON: %s", str(e))
        return {
            "statusCode": 400,
            "body": json.dumps(
                {
                    "status": "error",
                    "message": "Invalid provider_number",
                }
            ),
        }
    except Exception as e:
        logger.error("Error in lambda_handler", exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps(
                {
                    "status": "error",
                    "message": f"Internal server error: {str(e)}",
                }
            ),
        }
