import sys
import logging
import json
import base64
import boto3
import pytz
import pymysql
from datetime import datetime
from botocore.exceptions import ClientError
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import current_timestamp, col, lower, udf, regexp_replace, lpad
from pyspark.sql.types import StringType

#Configure logging.
logging.basicConfig(level=logging.INFO)
#set the logging level to INFO
logger = logging.getLogger("claimant-billingestion-sa-bill-file-generation-job")

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
        logger.error("Error retrieving secret '%s': %s", secret_name, str(e))
        raise e
    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response.get('SecretString', '')
    if not secret: 
        decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
        secret = decoded_binary_secret
    return json.loads(secret)

def retrive_names_udf(name,arg):
    last_name=''
    first_name=''
    middle_name=''
    return_name=''
    name=name
    if name.strip().find(",") > 0:
        last_name=name.split(',')[0].strip()
        f_m_name=name.split(',')[1].strip().split(' ')
        first_name=f_m_name[0]
        middle_name=''
        if len(f_m_name) == 2:
            middle_name=f_m_name[1]
        if len(f_m_name) > 2:
          f_m_name.pop(0)
          middle_name=' '.join(f_m_name)
        
    else:
        last_name=name
        
    if arg=='first_name':
        return_name=first_name
    elif arg=='last_name':
        return_name=last_name
    else:
        return_name=middle_name
    return return_name
        
def get_df_db_data_with_query(read_db_sql, secrets, df_num_partitions=100):
    try:
        jdbc_url = f'jdbc:mysql://{secrets["host"]}:{secrets["port"]}/{secrets["dbClusterIdentifier"]}'
        read_db_sql = f"""({read_db_sql}) AS db_vw"""
        logger.info(f"read_db_sql => {read_db_sql}")
        df_db_res = spark.read.format("jdbc") \
                        .option("url", jdbc_url) \
                        .option("dbtable", read_db_sql) \
                        .option("user", secrets["username"]) \
                        .option("password", secrets["password"]) \
                        .option("numPartitions", "5") \
                        .option("fetchsize", "1000") \
                        .load()
        if df_db_res.count() == 0:
            logger.info(f"No matching records found for input read db sql!!!")
        else:
            logger.info(f"df_db_res partition count => {df_db_res.rdd.getNumPartitions()}")
            logger.info(f"Setting df_db_res partition count as => {df_num_partitions}")
            df_db_res = df_db_res.repartition(df_num_partitions)
            logger.info(f"df_db_res NEW partition count => {df_db_res.rdd.getNumPartitions()}")
        return df_db_res
    except Exception as e:
        logger.error("Error executing DB reading: %s", str(e))
        raise e

def update_db_records(updated_df, secrets, main_table_name, stage_table_name, update_db_query):
    """
    Update records in the main table based on data from a stage table using provided query.
    """
    # Extracting values from secrets
    host = secrets["host"]
    database = 'txm_bitx'  
    user = secrets["username"]
    password = secrets["password"]
    port = secrets["port"]
    logger.info(f"Database: {database}")
    logger.info(f"Main Table Name: {main_table_name}")
    logger.info(f"Stage Table Name: {stage_table_name}")

    try:
        # Write updated_df to the stage table in the database
        updated_df.write.format("jdbc") \
            .option("url", f"jdbc:mysql://{host}:{port}/{database}") \
            .option("dbtable", stage_table_name) \
            .option("user", user) \
            .option("password", password) \
            .mode("overwrite") \
            .save()
            
        logger.info(f"Successfully wrote updated_df data to stage table: {stage_table_name}")
        logger.info(f"update_db_query => {update_db_query}")

        # Establish MySQL connection
        connection = pymysql.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        )
        with connection.cursor() as cursor:
            # Execute the provided query
            cursor.execute(update_db_query)
            connection.commit()
        logger.info("Query executed successfully.")

        # Drop the stage table (if it exists)
        with connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {stage_table_name}")
            connection.commit()
        logger.info(f"Dropped stage table: {stage_table_name}")
    except Exception as e:
        logger.error("Error executing DB Update Records Flow: %s", str(e))
        raise e
    finally:
        # Close MySQL connection
        if connection:
            connection.close()
        logger.info("Closed MySQL connection.")
        
def log_and_email(env_profile,errorInfo,errormessage,severity,sns_topic_arn):
     """logging error and sending error email"""
     logger.info("inside log email method")
     cst=pytz.timezone('America/Chicago')
     timestamp=datetime.now(cst).strftime('%Y/%m/%d %H:%M:%S %Z%z')
     #create log message
     process="Process : Bill File Generation"
     errInfo=f"Error  : {errorInfo}"
     time=f"Timestamp : {timestamp}"
     errLog=f"Fields & Content from Error log : {errormessage}"
     log_message=f"{errInfo}\n{time}"
     logger.error(log_message)
     subject=f"{env_profile} - {errorInfo} -{severity}"
     
     response=sns_client.publish(
        TopicArn=sns_topic_arn,
        Subject=subject,
        Message=f"{process}\n{errInfo}\n{time}\n{errLog}"
        )
     return response

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
        connect_timeout = 3600
    )
    try:
        cursor = conn.cursor()
        out_param_value = None
        
        logger.info(f"Calling DB Stored Procedure {db_stored_procedure}...")
        logger.info(f"db_sp_params => {db_sp_params}")
        cursor.execute('SET SESSION max_execution_time = 3600')
        cursor.execute('SET LOCAL net_read_timeout = 31536000')
        cursor.execute('SET LOCAL wait_timeout = 28800')
        cursor.execute('SET LOCAL interactive_timeout = 28800')
        cur_res = cursor.callproc(db_stored_procedure, db_sp_params)
        logger.info(f"cur_res => {cur_res}")
        logger.info(f"Stored Procedure Executed!!!")
        logger.info(f"Stored Procedure execution output:")
        logger.info(cursor.fetchall()) ###First resultset
        while cursor.nextset(): ###Remaining resultset
            for res in cursor.fetchall():
                logger.info(res)
                if any(str.startswith('ProcedureExecutionError:') for str in res):
                    raise Exception(f"Error while executing Stored Procedure {db_stored_procedure}!!!")
        if return_out_param_position:
            out_param_query = f'SELECT @_{db_stored_procedure}_{return_out_param_position}'
            logger.info(f"out_param_query => {out_param_query}")
            cur_res = cursor.execute(out_param_query)
            logger.info(f"cur_res => {cur_res}")
            out_res = cursor.fetchone() ###First resultset
            logger.info(f"out_res => {out_res}")
            if out_res:
                out_param_value = out_res[0]
        logger.info(f"out_param_value => {out_param_value}")
        conn.commit()
    except Exception as e:
        logger.error("Error executing DB Stored Procedure: %s", str(e))
        raise e
    finally:
        conn.close()
    return out_param_value     


def rename_s3_file(bucket_name,sa_file_path,new_file_name,new_file_name_seq):
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=sa_file_path)
    if 'Contents' in response and response['Contents']:
        files = response['Contents']
        files_sorted = sorted(files, key=lambda x: x['LastModified'], reverse=True)
        target_file = files_sorted[0]["Key"]
        logger.info(f'Target_FILE : {target_file}')
        if target_file:
            new_file_name = new_file_name.replace('<date_format>',new_file_name_seq)
            logger.info('new_file_name',new_file_name)
            new_file_rename=sa_file_path+new_file_name
            logger.info('new file name after rename',new_file_rename)
            copy_source = {'Bucket': bucket_name, 'Key': target_file}
            s3_client.copy_object(CopySource=copy_source, Bucket=bucket_name, Key=new_file_rename)
            s3_client.delete_object(Bucket=bucket_name, Key=target_file)
            logger.info(f"Renamed File {target_file} to {new_file_rename}")
    return True

def update_invoice_status(input_df, secrets, status_type, status):
    logger.info(f"Invoice Status table update STARTED for status_type {status_type} and status {status}")
    v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

    update_main_table_name = f"txm_bitx.invoice_status"
    logger.info(f"update_main_table_name => {update_main_table_name}")

    update_stage_table_name = f"{update_main_table_name}_stage"
    logger.info(f"update_stage_table_name => {update_stage_table_name}")

    update_query = f"""
            UPDATE {update_main_table_name} main
            JOIN {update_stage_table_name} stage ON main.txm_invoice_number = stage.txm_invoice_number
            SET main.status='SUBMITTED',update_user='Glue-ETL-Bill Feed',update_timestamp=NOW(6)
            WHERE status_type='ADJUDICATION'
            """
    logger.info(f"update query--> {update_query}")
    logger.info(f"Calling update_db_records function to update {update_main_table_name} in database...")
    update_db_records(input_df, secrets, update_main_table_name, update_stage_table_name, update_query)
    logger.info(f"Invoice Status table update COMPLETED for status_type {status_type} and status {status}")
    
    logger.info(f"Invoice Step Status table load STARTED for status_type {status_type} and status {status}")
    update_main_table_name = f"txm_bitx.invoice_step_status"
    logger.info(f"update_main_table_name => {update_main_table_name}")

    update_stage_table_name = f"{update_main_table_name}_stage"
    logger.info(f"update_stage_table_name => {update_stage_table_name}")

    insert_query = f"""
        INSERT INTO {update_main_table_name} (status_id, status_type, step_info, step_status, create_user, update_user, create_timestamp, update_timestamp)
        SELECT s.status_id, '{status_type}', '{status}', '{status}', 'Glue-ETL-Bill Feed', 'Glue-ETL-Bill Feed', '{v_load_timestamp}', '{v_load_timestamp}'
        FROM txm_bitx.invoice_status s
        JOIN {update_stage_table_name} stage ON s.txm_invoice_number = stage.txm_invoice_number
    """
    logger.info(f"Calling update_db_records function to insert {update_main_table_name} in database...")
    update_db_records(input_df, secrets, update_main_table_name, update_stage_table_name, insert_query)
    logger.info(f"Invoice Step Status table load COMPLETED for status_type {status_type} and status {status}")





args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'db_secret_key',
    'bucket_name',
    'env_profile',
    'sns_topic_arn'
])

#Create GlueContext, SparkContext, and SparkSession
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job=Job(glueContext)
job.init(args['JOB_NAME'],args)
job_id = args['JOB_RUN_ID']
db_secret_key=args['db_secret_key']
bucket_name=args['bucket_name']
env_profile=args['env_profile']
sns_topic_arn=args['sns_topic_arn']
try:
    secrets = get_secret(db_secret_key, 'us-east-1')
    sns_client=boto3.client('sns',region_name='us-east-1')
    #logging 
    db_sp_params = job_id, 'Glue-ETL-Bill Feed', 'Glue-ETL-Bill Feed',0
    logger.info(f"db_sp_params => {db_sp_params}")
    dbStoredProcedure = "txm_bitx_enrichment_logs.insert_bill_sa_logs"
    logger.info(f"dbStoredProcedure => {dbStoredProcedure}")
    batch_id = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, 3)
    logger.info(f"batch_id => {batch_id}")
    
    AWSGlueDataCatalog_SA_Bill_iitc_lkp = glueContext.create_dynamic_frame.from_catalog(database="bitx_static_config", table_name="image_invoice_type_control", transformation_ctx="AWSGlueDataCatalog_SA_Bill_iitc_lkp")
    AWSGlueDataCatalog_SA_Bill_iitc_lkp.printSchema()
    AWSGlueDataCatalog_SA_Bill_iitc_lkp.show()
    provider_iitc_lkp_config_df = AWSGlueDataCatalog_SA_Bill_iitc_lkp.toDF()
    provider_iitc_lkp_config_df.cache()
    provider_iitc_lkp_config_df.show()
    provider_iitc_lkp_config_df.createOrReplaceTempView("bill_iitc_lkp_config_vw")
    
    AWSGlueDataCatalog_SA_Bill_typ_lkp = glueContext.create_dynamic_frame.from_catalog(database="bitx_static_config", table_name="bill_type_reference", transformation_ctx="AWSGlueDataCatalog_SA_Bill_type_lkp")
    AWSGlueDataCatalog_SA_Bill_typ_lkp.printSchema()
    AWSGlueDataCatalog_SA_Bill_typ_lkp.show()
    provider_lkp_config_df = AWSGlueDataCatalog_SA_Bill_typ_lkp.toDF()
    provider_lkp_config_df.cache()
    provider_lkp_config_df.show()
    provider_lkp_config_df.createOrReplaceTempView("bill_typ_lkp_config_vw")
    
    
    bill_type_join_df = spark.sql(f"""SELECT DISTINCT image_document_id
                                             ,batch_type
                                             ,COALESCE(translation_value,'') AS translation_value
                                             ,status_department AS status
                                              FROM bitx_static_config.image_invoice_type_control iit 
                                              LEFT OUTER JOIN bitx_static_config.bill_type_reference btf ON btf.code=iit.batch_type""")
    bill_type_join_df.cache()
    bill_type_join_df.show()
    bill_type_join_df.createOrReplaceTempView("bill_type_join_df_vw")                                         
    
    #Register UDF   
    spark.udf.register("retrive_names_udf", retrive_names_udf,StringType())
    #Reading master configuration file
    AWSGlueDataCatalog_SA_master = glueContext.create_dynamic_frame.from_catalog(database="bitx_sa_mappings", table_name="sa_bill_mapping_master", transformation_ctx="AWSGlueDataCatalog_SA_master")
    AWSGlueDataCatalog_SA_master.printSchema()
    AWSGlueDataCatalog_SA_master.show()
    master_config_df = AWSGlueDataCatalog_SA_master.toDF()
    master_config_df.show()
    iterator_m=master_config_df.rdd.toLocalIterator()
    datacatalog_schema_name=''
    datacatalog_validation_table_name=''
    df_table_name=''
    db_read_sql_select=''
    sa_file_path=''
    new_file_name=''
    status='SUCCESS'
    errorInfo=''
    errormessage=''
    provider_file_path=''
    max_batch_number=''
    batch_number_init=0
    new_file_name_seq=''
    for rowm in iterator_m:
        datacatalog_schema_name=str(rowm['datacatalog_schema_name']).strip()
        datacatalog_validation_table_name=str(rowm['datacatalog_bill_mapping_table_name']).strip()
        df_table_name_head=str(rowm['df_table_name_head']).strip()
        df_table_name_detail=str(rowm['df_table_name_detail']).strip()
        db_read_sql_select_head=str(rowm['db_read_sql_select_head']).strip()
        db_read_sql_select_detail=str(rowm['db_read_sql_select_detail']).strip()
        sa_file_path=str(rowm['s3_file_prefix']).strip()
        new_file_name=str(rowm['outbound_file_name']).strip()
        max_batch_number_sql=str(rowm['max_batch_number']).strip()
        batch_number_init=int(rowm['batch_number'])
        db_read_sql_select_bu=str(rowm['db_read_sql_select_bu']).strip()
        df_table_name_bu=str(rowm['df_table_name_bu']).strip()
    
    read_sql_head=db_read_sql_select_head
    read_sql_detail=db_read_sql_select_detail
    read_sql_bu=db_read_sql_select_bu
    logger.info(f"sql from master config head => {read_sql_head}")
    logger.info(f"sql from master config detail => {read_sql_detail}")
    bill_header_list_df = get_df_db_data_with_query(read_sql_head, secrets)
    bill_detail_list_df = get_df_db_data_with_query(read_sql_detail, secrets)
    bill_bu_list_df = get_df_db_data_with_query(read_sql_bu, secrets)
    max_batch_number_frm_db_df = get_df_db_data_with_query(max_batch_number_sql, secrets)
    max_batch_number_frm_db_df.show(1,truncate=False)
    logger.info(f"batch_number from file for start--> {batch_number_init}")
    iterator_n=max_batch_number_frm_db_df.rdd.toLocalIterator()
    for rown in iterator_n:
        max_batch_number=int(rown['max_file_number'])
    logger.info(f"batch_number from db-->{max_batch_number}")
    # Validating Provide Count and process for mapping file
    bill_header_count=bill_header_list_df.count()
    logger.info('Bill Header Records counts for file generation-->',bill_header_count)
    julien_date=str(datetime.now().strftime('%y'))+str(datetime.now().timetuple().tm_yday).zfill(3)
    if bill_header_count > 0:
        logger.info('Data is available for Bill Header Mapping !!!')
        bill_header_list_df.cache()
        bill_detail_list_df.cache()
        AWSGlueDataCatalog_MappingConfig = glueContext.create_dynamic_frame.from_catalog(database="bitx_sa_mappings", table_name="sa_mapping_bill_config", transformation_ctx="AWSGlueDataCatalog_MappingConfig")
        AWSGlueDataCatalog_MappingConfig.printSchema()
        config_df = AWSGlueDataCatalog_MappingConfig.toDF()
        config_df.show(5,truncate=False)
        config_df_hd=config_df.filter(config_df.record_type=='HD')
        config_df_hd.show(5,truncate=False)
        config_df_bh=config_df.filter(config_df.record_type=='BH')
        config_df_bh.show(5,truncate=False)
        config_df_bd=config_df.filter(config_df.record_type=='BD')
        config_df_bd.show(5,truncate=False)
        config_df_bu=config_df.filter(config_df.record_type=='BU')
        config_df_bu.show(5,truncate=False)
        
        iterator_hd=config_df_hd.orderBy(['field_order']).rdd.toLocalIterator()
        iterator_bh=config_df_bh.orderBy(['field_order']).rdd.toLocalIterator()
        iterator_bd=config_df_bd.orderBy(['field_order']).rdd.toLocalIterator()
        iterator_bu=config_df_bu.orderBy(['field_order']).rdd.toLocalIterator()
        logics_hd=[]
        logics_bh=[]
        logics_bd=[]
        logics_bu=[]
    
        for rowh in iterator_hd:
            logics_hd.append(rowh['sql_mapping_logic'])    
        logger.info("Count of Header Elements from Mapping",len(logics_hd))
        
        for rowbh in iterator_bh:
            logics_bh.append(rowbh['sql_mapping_logic'])    
        logger.info("Count of Bill Header Elements from Mapping",len(logics_bh))
        
        for rowbd in iterator_bd:
            logics_bd.append(rowbd['sql_mapping_logic'])    
        logger.info("Count of Bill Detail Elements from Mapping",len(logics_bd))
        
        
        for rowbu in iterator_bu:
            logics_bu.append(rowbu['sql_mapping_logic'])    
        logger.info("Count of Bill Detail Elements from Mapping",len(logics_bu))
    
        final_query_hd=','.join(logics_hd)
        logger.info("Header Query--->",final_query_hd)   
            
        final_query_bh=','.join(logics_bh)
        logger.info("Bill Header Query-->",final_query_bh)
        
        final_query_bd=','.join(logics_bd)
        logger.info("Bill Detail Query-->",final_query_bd)
        
        final_query_bu=','.join(logics_bu)
        logger.info("Bill BU Query-->",final_query_bu)
        
        
        # Bill Header Logic
        bill_header_list_df.createOrReplaceTempView(df_table_name_head)
        bill_header_list_df.printSchema()
        bill_header_list_df.show(5, truncate=False)
        bill_header_list_df=bill_header_list_df.join(bill_type_join_df,bill_header_list_df.form_type ==  bill_type_join_df.image_document_id,"left")
        bill_header_list_df.createOrReplaceTempView("bill_header_list_vw")
        bill_header_list_df.printSchema()
        bill_header_list_df.show()
        bill_header_list_df.show(3,truncate=False)
        bill_type_join_df_ECI_EBILL = spark.sql(f"""SELECT DISTINCT bill_id,
                                                            trading_partner,
                                                            form_type,
                                                            file_header_id,
                                                            source,
                                                            facility_code,
                                                            bill_frequency_type_code,
                                                            Client_Code,
                                                            admission_source,
                                                            admission_type,
                                                            admission_date,
                                                            admission_hour,
                                                            CASE WHEN TRIM(UPPER(form_type))=TRIM(UPPER(image_document_id)) AND TRIM(UPPER(status)) IN ('E') THEN 'C' 
                                                            WHEN TRIM(UPPER(form_type))='RSBILL3' AND TRIM(UPPER(status)) IN('S') THEN 'C' 
                                                            ELSE 'P' END AS App_Benefits,
                                                            Already_Repriced_Flag,
                                                            diagnosis_codes,
                                                            ICD_Procedure_Code,
                                                            txm_invoice_number,
                                                            txm_claim_number,
                                                            Client_TOB,
                                                            discharge_date,
                                                            discharge_hour,
                                                            date_of_injury,
                                                            pps_code,
                                                            Due_Date,
                                                            provider_number,
                                                            Retail_PPO_Fees,
                                                            Fee_Override,
                                                            Patient_Account_Number,
                                                            Patient_Status,
                                                            CASE WHEN TRIM(UPPER(form_type))='RSBILL3' AND TRIM(UPPER(status)) IN('S') THEN '3RDP' 
                                                            WHEN TRIM(UPPER(form_type)) IN ('HCFA3RD','UB3RD','DENT3RD','RX3RD') AND TRIM(UPPER(status)) IN('E') THEN '3RDP'
                                                            WHEN TRIM(UPPER(form_type)) NOT IN ('HCFA3RD','UB3RD','DENT3RD','RX3RD') AND TRIM(UPPER(status)) IN('E') THEN 'REIM'
                                                            ELSE '0' END AS Payment_Auth,
                                                            place_of_service_code,
                                                            PPO_Contract_ID,
                                                            PPO_Network_ID,
                                                            Rendering_Provider_License_Number,
                                                            Provider_Medicare_Number,
                                                            Rendering_Provider_Type,
                                                            Rendering_Provider_Zip,
                                                            Reeval_Flag,
                                                            Repriced_Review_Date,
                                                            Bill_Review_Received_Date,
                                                            Received_Date,
                                                            Submit_Date,
                                                            Referring_Provider_Name,
                                                            CASE WHEN LOWER(source)='eci' AND TRIM(UPPER(batch_type)) <> 'H' THEN COALESCE(translation_value,'') 
                                                                 WHEN LOWER(source)='eci' AND TRIM(UPPER(batch_type)) ='H' THEN 'O' 
                                                            ELSE COALESCE(TOB_Type_of_Bill,'') END AS TOB_Type_of_Bill,
                                                            type_of_service,
                                                            UB04_TOB_type_of_Bill,
                                                            Billing_Pay_To_Provider_Billing_Address_one,
                                                            Billing_Pay_To_Provider_Billing_Address_two,
                                                            Billing_Pay_To_Provider_Billing_City,
                                                            Billing_Pay_To_Provider_Billing_Phone,
                                                            Billing_Pay_To_Provider_Billing_State,
                                                            Billing_Pay_To_Provider_Billing_Zip,
                                                            Billing_Pay_To_Provider_Group,
                                                            Billing_Pay_To_Provider_Practice_Address_one,
                                                            Billing_Pay_To_Provider_Practice_Address_two,
                                                            Billing_Pay_To_Provider_Practice_City,
                                                            Billing_Pay_To_Provider_Practice_State,
                                                            Billing_Pay_To_Provider_Practice_Zip,
                                                            Billing_Pay_To_Provider_TIN,
                                                            COALESCE(retrive_names_udf(Rendering_Provider_First_Name,'first_name'),'') AS Rendering_Provider_First_Name,
                                                            COALESCE(retrive_names_udf(Rendering_Provider_Last_Name,'last_name'),'') AS Rendering_Provider_Last_Name,
                                                            COALESCE(retrive_names_udf(Rendering_Provider_Middle_Initial,''),'') AS Rendering_Provider_Middle_Initial,
                                                            Provider_Signature_on_File,
                                                            Principal_Procedure_Code_Date,
                                                            Other_Procedure_Code_Date_2,
                                                            Other_Procedure_Code_Date_3,
                                                            Other_Procedure_Code_Date_4,
                                                            Other_Procedure_Code_Date_5,
                                                            Other_Procedure_Code_Date_6,
                                                            COALESCE(retrive_names_udf(Referring_Provider_First_Name,'first_name'),'') AS Referring_Provider_First_Name,
                                                            COALESCE(retrive_names_udf(Referring_Provider_Middle_Name,''),'') AS Referring_Provider_Middle_Name,
                                                            COALESCE(retrive_names_udf(Referring_Provider_Last_Group_Name,'last_name'),'') AS Referring_Provider_Last_Group_Name,
                                                            Referring_Provider_DEA_Number,
                                                            Referring_Provider_License_Number,
                                                            Admitting_Diagnosis_Code,
                                                            facility_npi,
                                                            Billing_Provider_NPI,
                                                            Attending_Rendering_Provider_NPI,
                                                            Operating_Physician_NPI,
                                                            External_Cause_of_Injury_1_3,
                                                            Occurrence_Code_1,
                                                            Occurrence_Date_1,
                                                            Occurrence_Code_2,
                                                            Occurrence_Date_2,
                                                            Occurrence_Code_3,
                                                            Occurrence_Date_3,
                                                            Occurrence_Code_4,
                                                            Occurrence_Date_4,
                                                            referring_provider_npi,
                                                            Operating_Physician_License_Number,
                                                            diagnosis_codes_1,
                                                            Diagnosis_Code_18,
                                                            MPN_HCN_Insurer_Sequence_Number,
                                                            Bill_ICD_Version,
                                                            translation_value
                                                            FROM bill_header_list_vw""")
        logger.info("ECI/EBILL TOB")
        bill_type_join_df_ECI_EBILL.show(5,truncate=False)                                                    
        bill_header_list_count = bill_header_list_df.count()
        bill_type_join_df_ECI_EBILL.createOrReplaceTempView("bill_type_join_ECI_EBILL_vw")
        logger.info(f"Bill header list Record Count => {bill_header_list_count}")
        BH_df=spark.sql("select Concat("+final_query_bh+") AS final,2 as seq,txm_invoice_number AS txm_invoice_number from bill_type_join_ECI_EBILL_vw")
        logger.info('Sample data for bill header')
        BH_df.show(5,truncate=False)
        
        #Bill Detail logic
        bill_detail_list_df=bill_detail_list_df.orderBy(["bill_id","line_seq"], ascending=[True,True])
        bill_detail_list_df.createOrReplaceTempView(df_table_name_detail)
        bill_detail_list_df.printSchema()
        bill_detail_list_df.show(5, truncate=False)
        bill_detail_list_cnt = bill_detail_list_df.count()
        logger.info(f"Bill detail list Record Count => {bill_detail_list_cnt}")
        BD_df=spark.sql("select Concat("+final_query_bd+") AS final,3 as seq,Import_Bill_ID  AS txm_invoice_number from "+df_table_name_detail)
        BD_df.printSchema()
        logger.info('Sample data below for Detail')
        BD_df.show(5, truncate=False)
        
        
        #Bill BU logic
        bill_bu_list_df.createOrReplaceTempView(df_table_name_bu)
        bill_bu_list_df.printSchema()
        bill_bu_list_df.show(5, truncate=False)
        bill_bu_list_cnt = bill_bu_list_df.count()
        logger.info(f"Bill detail BU Record Count => {bill_bu_list_cnt}")
        BU_df=spark.sql("select Concat("+final_query_bu+") AS final,4 as seq,bill_dcn  AS txm_invoice_number from "+df_table_name_bu)
        BU_df.printSchema()
        logger.info('Sample data below for BU')
        BU_df.show(5, truncate=False)
        
         #File header Logic
        HD_df=spark.sql("select Concat("+final_query_hd+") AS final,1 as seq,0 as txm_invoice_number")
        HD_df.printSchema()
        logger.info('Sample data below for Header')
        HD_df.show(5, truncate=False)
        bill_header_record_count=str(bill_header_list_count).zfill(7)
        bill_detail_record_count=str(bill_detail_list_cnt).zfill(7)
        bill_bu_record_count=str(bill_bu_list_cnt).zfill(7)
        total_record_count=str(bill_header_list_count+bill_detail_list_cnt+bill_bu_list_cnt+1).zfill(7)
        HD_df=HD_df.withColumn('final', regexp_replace('final', 'Rec_cnt', str(total_record_count))).withColumn('final', regexp_replace('final', 'bill_cn', str(bill_header_record_count))).withColumn('final', regexp_replace('final', 'dtl_cnt', str(bill_detail_record_count))).withColumn('final', regexp_replace('final', 'udf_cnt', str(bill_bu_record_count)))
        bill_sa_df=HD_df.union(BH_df).union(BD_df).union(BU_df)
        bill_sa_df.printSchema()
        bill_sa_df=bill_sa_df.orderBy(["txm_invoice_number","seq"], ascending=[True,True])
        logger.info('Sample data before loading into file')
        bill_sa_df.select("seq","txm_invoice_number").show(10,truncate=False)
        bill_sa_df=bill_sa_df.drop(bill_sa_df.seq).drop(bill_sa_df.txm_invoice_number)
        bill_sa_df.printSchema()
        uniq_txm_in_num_df=bill_header_list_df.select('txm_invoice_number').distinct()
        logger.info('Txm Invoice Number process for update')
        uniq_txm_in_num_df.show(5,truncate=False)
        logger.info('Below are the Sample unique txm invoice number for updation/processed!')
        bill_file_path='s3://'+bucket_name+'/'+sa_file_path
        bill_sa_df.coalesce(1).write.option('header',False).option('lineSep','\r\n').mode('append').format('text').save(bill_file_path) 
        logger.info('File is uploaded into S3Path Successfully!')
        logger.info('bucket_name',bucket_name)
        logger.info('prefix',sa_file_path)
        if(max_batch_number==batch_number_init):
            max_batch_number=max_batch_number+1
        elif(max_batch_number  < batch_number_init):
            max_batch_number=batch_number_init+1
        elif(max_batch_number > batch_number_init):
            max_batch_number=max_batch_number+1;
        max_batch_number=str(max_batch_number).zfill(4)
        new_file_name_seq=julien_date+max_batch_number
        logger.info(f"New file number to process!==>{new_file_name_seq} ")
        batch_number_df = spark.createDataFrame([[job_id]], ["job_id"])
        file_response=rename_s3_file(bucket_name,sa_file_path,new_file_name,new_file_name_seq)
        if file_response:
            status_type = 'ADJUDICATION'
            status = 'SUBMITTED'
            update_invoice_status(uniq_txm_in_num_df, secrets, status_type, status)
            #updating logging table with latest batch number
            update_main_table_name = "txm_bitx_enrichment_logs.batch_bill_file_master"
            logger.info(f"update_main_table_name_logging => {update_main_table_name}")
            update_stage_table_name = f"{update_main_table_name}_stage"
            logger.info(f"update_stage_table_name_logging => {update_stage_table_name}")
            update_query = f"""
            UPDATE {update_main_table_name} main
            JOIN {update_stage_table_name} stage ON main.job_id = stage.job_id
            SET batch_number_file={max_batch_number}
            """
            logger.info(f"update query for logging--> {update_query}")
            logger.info(f"Calling update_db_records function to update {update_main_table_name} in database...")
            update_db_records(batch_number_df, secrets, update_main_table_name, update_stage_table_name, update_query)

    else:
        logger.info('No Bill Records to Process!')
        batch_number_df = spark.createDataFrame([[job_id]], ["job_id"])
        update_main_table_name = "txm_bitx_enrichment_logs.batch_bill_file_master"
        logger.info(f"update_main_table_name_logging => {update_main_table_name}")
        update_stage_table_name = f"{update_main_table_name}_stage"
        logger.info(f"update_stage_table_name_logging => {update_stage_table_name}")
        update_query = f"""
        UPDATE {update_main_table_name} main
        JOIN {update_stage_table_name} stage ON main.job_id = stage.job_id
        SET batch_number_file={max_batch_number}
        """
        logger.info(f"update query for logging--> {update_query}")
        logger.info(f"Calling update_db_records function to update {update_main_table_name} in database...")
        update_db_records(batch_number_df, secrets, update_main_table_name, update_stage_table_name, update_query)
except Exception as e:
    logger.error(e)
    errorInfo='Aborted Bill File Generation'
    errormessage=str(e)[:500]
    log_and_email(env_profile,errorInfo,errormessage,"FATAL",sns_topic_arn)
    status="FAILED"
    raise Exception("Aborted File Generation !!")
finally:
   logger.info('Finally Block')
   db_sp_params = batch_id,status, 'Glue', errorInfo, errormessage, 'Glue-ETL-Bill Feed', 'Glue-ETL-Bill Feed'
   logger.info(f"db_sp_params => {db_sp_params}")
   dbStoredProcedure = "txm_bitx_enrichment_logs.update_bill_SA_log_status"
   logger.info(f"dbStoredProcedure => {dbStoredProcedure}")
   res = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, None)
   logger.info('DB Response',res)
job.commit()