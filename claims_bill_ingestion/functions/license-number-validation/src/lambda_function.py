import pandas as pd
import sqlite3
import boto3
import io
import os
import re
from datetime import datetime

BUCKET_NAME = os.environ['BUCKET_NAME']
s3_client = boto3.client("s3")

def match(expr, item):
    print(f"match called with expr: {expr}, item: {item}")
    return re.match(expr, item) is not None

def read_csv_from_s3(bucket_name, file_key):
    obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    return pd.read_csv(io.StringIO(obj["Body"].read().decode("utf-8")))

def lambda_handler(event, context):
    # Input
    input_license_number = event.get("license_number", "")

    # Initializations
    result_obj = dict()

    # Check length of Input License Number
    if len(input_license_number) > 20:
        result_obj = {
            "license_number": input_license_number,
            "validation_result": "FAIL",
            "success_reason": "",
            "failure_reason": "License number must be 20 characters or fewer"
        }
        return result_obj

    # Initializse license_number with only space stripping 
    license_number = input_license_number.strip()
    print(f"license_number => {license_number}")

    # If License number empty, stoping the further process.
    if not license_number:
        result_obj = {
            "license_number": input_license_number,
            "validation_result": "FAIL",
            "success_reason": "",
            "failure_reason": "License Number is Empty"
        }
        return result_obj

    # Check for special characters - only allow alphanumeric and spaces
    if not match(r'^[A-Za-z0-9 ]+$', license_number):
        result_obj = {
            "license_number": input_license_number,
            "validation_result": "FAIL",
            "success_reason": "",
            "failure_reason": "Invalid license - Special Characters are not allowed"
        }
        return result_obj
    
    # Bypass validations for other state licenses
    license_number_suffix = license_number[-2:]
    if match('^[A-Z]*$', license_number_suffix) and license_number_suffix != "TX":
        result_obj = {
            "license_number": input_license_number,
            "validation_result": "PASS",
            "success_reason": "Unknown other state license format",
            "failure_reason": ""
        }
        return result_obj

    # License Validation for TX state starts here
    # Initialize SQLITE3 connection here
    conn = sqlite3.connect(":memory:")

    # Creating the 'MATCHES' function, it will call python 'match' function from sql to perform re.match.
    conn.create_function("MATCHES", 2, match)

    license_number = re.sub(r"TX$", "", license_number) if len(license_number) > 4 else license_number
    #print(f"license_number => {license_number}")
    # License Number with 20 characters right padding to mimic the validation logic followed in iSeries
    license_number = license_number.ljust(20, ' ')
    print(f"new license_number with padding => {license_number}")

    # Read files from S3
    account_verification_file_path = os.environ['ACCOUNT_VERIFICATION_DATA']
    print("account_verification_file_path:", account_verification_file_path)
    hc_provider_prefix_file_path = os.environ['HC_PROVIDER_PREFIX']
    print("hc_provider_prefix_file_path:", hc_provider_prefix_file_path)
    account_verification_df = read_csv_from_s3(BUCKET_NAME, account_verification_file_path)
    hc_provider_prefix_df = read_csv_from_s3(BUCKET_NAME, hc_provider_prefix_file_path)
    
    # Create pandas SQL view
    account_verification_df.to_sql("account_verification", conn, index=False, if_exists="replace")
    hc_provider_prefix_df.to_sql("hc_provider_prefix", conn, index=False, if_exists="replace")

    # Process license_number
    prefix_3_digits = license_number[:3]
    prefix_2_digits = license_number[:2]
    current_date = datetime.now().strftime('%Y-%m-%d')
    
    # SQL query execution
    query = f"""
    WITH formatted_dates AS (
        SELECT 
            provider_prefix,
            DATE(substr(effective_date, 1, 4) || '-' || substr(effective_date, 5, 2) || '-' || substr(effective_date, 7, 2)) AS effective_date,
            DATE(substr(expiration_date, 1, 4) || '-' || substr(expiration_date, 5, 2) || '-' || substr(expiration_date, 7, 2)) AS expiration_date
        FROM hc_provider_prefix
    ),
    matches_3_digits AS (
        SELECT provider_prefix, effective_date, expiration_date, '3_digit_match' AS match_type
        FROM formatted_dates
        WHERE ('{current_date}' >= effective_date AND '{current_date}' < expiration_date)
        AND provider_prefix = '{prefix_3_digits}'
    ),
    matches_2_digits AS (
        SELECT h.provider_prefix, h.effective_date, h.expiration_date, '2_digit_match' AS match_type
        FROM formatted_dates h
        LEFT JOIN matches_3_digits m3 ON h.provider_prefix = '{prefix_3_digits}'
        WHERE m3.provider_prefix IS NULL
        AND h.provider_prefix = '{prefix_2_digits}'
        AND ('{current_date}' >= h.effective_date AND '{current_date}' < h.expiration_date)
    )
    SELECT * FROM matches_3_digits
    UNION ALL
    SELECT * FROM matches_2_digits
    """
    license_matched_df = pd.read_sql(query, conn)
    print("Query executed for license_matched_df")

    print(license_matched_df.head())

    # Get row count
    license_matched_count = len(license_matched_df)
    print(f"license_matched_count => {license_matched_count}")

    # If rows exist, add substring transformations
    if license_matched_count > 0:
        columns_to_add = {
            "lic_1": license_number[:2],
            "lic_13": license_number[:3],
            "lic_104": license_number[:4],
            "lic_304": license_number[2:4],
            "lic_520": license_number[4:20].strip(' '),
            "lic_320": license_number[2:20].strip(' '),
            "lic_505": license_number[4:5],
            "lic_612": license_number[5:12],
            "lic_1320": license_number[12:20].strip(' '),
            "lic_506": license_number[4:6],
            "lic_507": license_number[4:7],
            "lic_720": license_number[6:20].strip(' '),
            "lic_308": license_number[2:8],
            "lic_920": license_number[8:20].strip(' '),
            "lic_309": license_number[2:9],
            "lic_1020": license_number[9:20].strip(' '),
            "lic_306": license_number[2:6],
            "lic_307": license_number[2:7],
            "lic_8": license_number[7:8],
            "lic_9": license_number[8:9],
            "lic_708": license_number[6:8],
            "lic_820": license_number[7:20].strip(' '),
            "lic_707": license_number[6:7],
            "lic_303": license_number[2:3],
            "lic_404": license_number[3:4],
            "lic_420": license_number[3:20].strip(' '),
            "lic_num": license_number[:20],
            "lic_809": license_number[7:9]
        }

        for col, value in columns_to_add.items():
            license_matched_df[col] = value
            print(f"Column: {col}, Value: {value}")

        # Add constant values separately
        license_matched_df["license_number"] = license_number
        print("Columns successfully added using dynamic mapping!")
        print("Substring transformations applied.")

        print(license_matched_df.head())

        license_matched_df.to_sql("license_matched", conn, index=False, if_exists="replace")
        query = """
                SELECT i.*,
                        CASE WHEN i.validation_result_reason = 'PASS' THEN i.validation_result_reason ELSE 'FAIL' END AS validation_result,
                        CASE WHEN i.validation_result_reason = 'PASS' THEN 'License validation completed.' ELSE '' END AS success_reason,
                        CASE WHEN i.validation_result_reason = 'PASS' THEN '' ELSE i.validation_result_reason END AS failure_reason
                FROM (SELECT 
                        *,
                        CASE
                            WHEN lic_13 IN ('AMB', 'ASC', 'CPS', 'CSW', 'LPC', 'LSA', 'NFA', 'PSY', 'CNS') 
                            OR lic_104 IN ('CRNA', 'CRAA') THEN 'Inactive provider license type'

                            WHEN lic_304 = 'TX' 
                                THEN CASE WHEN NOT MATCHES('^[A-Z]*$', lic_1) THEN 'Missing license prefix for TX' 
                                            WHEN (a.state_code IS NULL OR (lic_520 IS NOT NULL AND lic_520 <> '')) THEN 'Missing license prefix for TX'
                                            ELSE 'PASS'
                                        END

                            WHEN lic_1 = 'TM' THEN 'PASS'

                            WHEN lic_1 = 'PM' 
                                AND (NOT MATCHES('^[A-Z0-9 ]+$', lic_320) OR (lic_320 IS NULL OR lic_320 = '')) THEN 'Invalid License for PM'

                            WHEN lic_1 = 'IL' AND NOT MATCHES('^[A-Z]*$', lic_505) THEN 'License prefix IL must have an alphabetic character in position 5'

                            WHEN lic_1 = 'IL' AND ((NOT MATCHES('^[0-9]+$', lic_612))
                                    OR (NOT MATCHES('^[0-9]+$', lic_304))
                                    OR (state_code IS NULL) OR (lic_1320 IS NOT NULL AND lic_1320 <> '')) THEN 'License prefix IL must have digits in 6 to 12 or valid state in 3&4 if length is 4.' 

                            WHEN lic_104 = 'CRAA' 
                                AND (NOT MATCHES('^[0-9]+$', lic_506) OR (lic_720 IS NOT NULL AND lic_720 <> '')) THEN 
                                CASE 
                                    WHEN (NOT MATCHES('^[0-9]+$', lic_507) OR (lic_820 IS NOT NULL AND lic_820 <> '')) THEN 'License prefix CRAA must be followed by 2 or 3 digits'
                                    ELSE 'PASS'
                                END

                            WHEN (lic_1 = 'RN' OR lic_1 = 'CR' OR lic_1 = 'NP') 
                                AND (NOT MATCHES('^[0-9]+$', lic_308) OR (lic_920 IS NOT NULL AND lic_920 <> '')) THEN 
                                CASE 
                                    WHEN (NOT MATCHES('^[0-9]+$', lic_309) OR (lic_1020 IS NOT NULL AND lic_1020 <> '')) THEN 'License prefix RN, CR, or NP must be followed by 6 or 7 digits'
                                    ELSE 'PASS'
                                END

                            WHEN lic_1 = 'AM' 
                                AND (NOT MATCHES('^[0-9]+$', lic_308) OR (lic_920 IS NOT NULL AND lic_920 <> '')) THEN 
                                CASE 
                                    WHEN (NOT MATCHES('^[0-9]+$', lic_309) OR (lic_1020 IS NOT NULL AND lic_1020 <> '')) THEN 'License prefix AM must be followed by 6 or 7 digits'
                                    ELSE 'PASS'
                                END

                            WHEN (lic_1 = 'CP' OR lic_1 = 'PS') 
                                AND (NOT MATCHES('^[0-9]+$', lic_306) OR (lic_720 IS NOT NULL AND lic_720 <> '')) THEN 
                                CASE 
                                    WHEN (NOT MATCHES('^[0-9]+$', lic_307) OR (lic_820 IS NOT NULL AND lic_820 <> '')) THEN 'License prefix CP or PS must be followed by 4 or 5 digits'
                                    ELSE 'PASS'
                                END

                            WHEN (lic_1 = 'AS' OR lic_1 = 'CN' OR lic_1 = 'NF') 
                                AND (NOT MATCHES('^[0-9]+$', lic_308) OR (lic_920 IS NOT NULL AND lic_920 <> '')) THEN 'License prefix AS, CN, or NF must be followed by 6 digits' 

                            WHEN (lic_1 = 'DM' OR lic_1 = 'RA') 
                                AND LENGTH(TRIM(lic_num)) = 4 
                                AND (SELECT COUNT(1) 
                                    FROM account_verification a 
                                    WHERE a.state_code = SUBSTRING(TRIM(lic_num), 3, 2) ) = 0 THEN 'License Prefix DM/RA is missing valid state code' 

                            WHEN (lic_1 = 'DM' OR lic_1 = 'RA') 
                                AND LENGTH(TRIM(lic_num)) <> 4 THEN 'License prefix DM or RA must include a valid state code' 

                            WHEN lic_1 = 'LS' 
                                AND (NOT MATCHES('^[0-9]+$', lic_307) OR (lic_820 IS NOT NULL AND lic_820 <> '')) THEN 'License prefix LS must be followed by 5 digits'

                            WHEN lic_1 IN ('LP', 'CS', 'MS', 'OP') AND (lic_320 IS NULL OR lic_320 = '') THEN 'License prefix LP, CS, MS, or OP must be followed by digits'
                            WHEN lic_1 IN ('LP', 'CS', 'MS', 'OP') AND NOT MATCHES('^[0-9 ]*$', lic_320) THEN 'License prefix LP, CS, MS, or OP must be followed by digits'
                            WHEN lic_1 IN ('LP', 'CS', 'MS', 'OP') AND NOT MATCHES('^[0-9]+$', RTRIM(lic_320)) THEN 'License prefix LP, CS, MS, or OP must be followed by digits'
                            
                            WHEN (lic_1 = 'DO' OR lic_1 = 'MD') 
                                AND ((MATCHES('^[0-9]+$', lic_303) AND NOT MATCHES('^[A-Z0-9 ]+$', lic_420)) OR 
                                (NOT MATCHES('^[A-Z0-9 ]+$', lic_420) OR lic_420 IS NULL OR lic_420 = '')) THEN 'License prefix DO or MD must be followed by 4 digits'

                            WHEN lic_1 = 'OD' THEN 
                                CASE 
                                    WHEN NOT MATCHES('^[0-9]+$', lic_306) AND NOT MATCHES('^[0-9]+$', lic_307) THEN 'License prefix OD must be followed by 4 digits and 1 or 2 alphabetic characters'
                                    WHEN lic_707 = '' AND lic_8 <> '' AND lic_9 = '' THEN 'License prefix OD must be followed by 4 digits and 1 or 2 alphabetic characters'
                                    WHEN (NOT MATCHES('^[A-Z ]+$', lic_708) OR (lic_920 IS NOT NULL AND lic_920 <> '')) AND 
                                        (NOT MATCHES('^[A-Z ]+$',lic_809) OR (lic_1020 IS NOT NULL AND lic_1020 <> '')) THEN 'License prefix OD must be followed by 4 digits and 1 or 2 alphabetic characters'
                                    ELSE 'PASS'
                                END 

                            WHEN (lic_1 = 'DC' OR lic_1 = 'DS') 
                                AND (NOT MATCHES('^[0-9]+$', lic_306) OR (lic_720 IS NOT NULL AND lic_720 <> '')) THEN 
                                CASE
                                    WHEN (NOT MATCHES('^[0-9]+$', lic_307) OR (lic_820 IS NOT NULL AND lic_820 <> '')) THEN 'License Prefix DC/DS should be followed by 4 or 5 digits'  
                                    ELSE 'PASS'  
                                END

                            WHEN (lic_1 = 'DP') 
                                AND (NOT MATCHES('^[0-9]+$', lic_306) OR (lic_720 IS NOT NULL AND lic_720 <> '')) THEN 
                                CASE
                                    WHEN (NOT MATCHES('^[0-9]+$', lic_307) OR (lic_820 IS NOT NULL AND lic_820 <> '')) 
                                        AND (NOT MATCHES('^[0-9]+$', lic_308) OR (lic_920 IS NOT NULL AND lic_920 <> '')) THEN  'License Prefix DP should be followed by 4 or 5 or 6 digits'  
                                    ELSE 'PASS' 
                                END

                            WHEN lic_1 IN ('AU', 'MT')
                                AND (NOT MATCHES('^[0-9]+$', lic_308) OR LENGTH(lic_308) < 6 OR (lic_920 IS NOT NULL AND TRIM(lic_920) <> '')) THEN 'License Prefix AU/MT should be followed by 6 digits'
                            WHEN lic_1 = 'OT' 
                                AND (NOT MATCHES('^[0-9]+$', lic_308) OR LENGTH(lic_308) < 6 OR (lic_920 IS NOT NULL AND TRIM(lic_920) <> '') OR lic_303 NOT IN ('0', '1')) THEN 'License Prefix OT should be followed by 6 digits and 3rd position be 0 or 1'
                            WHEN lic_1 = 'PT' 
                                AND (NOT MATCHES('^[0-9]+$', lic_309) OR (lic_1020 IS NOT NULL AND lic_1020 <> '')) THEN 'License Prefix PT should be followed by numeric characters only'

                            WHEN (lic_1 = 'AC' OR lic_1 = 'PA')
                                AND (NOT MATCHES('^[A-Z0-9]+$', lic_307) OR (lic_820 IS NOT NULL AND lic_820 <> '')) THEN 'License Prefix AC/PA should be 5 digits only'

                            ELSE 'PASS'
                        END AS validation_result_reason
                    FROM license_matched l
                    LEFT JOIN account_verification a 
                        ON a.state_code = lic_304) i"""

        result_df = pd.read_sql(query, conn)
        print("Query executed for result_df")
        print(result_df.head())
        
        result_df['license_number'] = input_license_number
        result_df = result_df[['license_number','validation_result','success_reason','failure_reason']]
        result_obj = result_df.to_dict(orient="records")[0]
    else:
        result_obj = {
            "license_number": input_license_number,
            "validation_result": "FAIL",
            "success_reason": "",
            "failure_reason": "Invalid license prefix"
        }
    
    print(f"result_obj => {result_obj}")

    return result_obj