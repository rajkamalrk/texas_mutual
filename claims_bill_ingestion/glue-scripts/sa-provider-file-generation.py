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
from pyspark.sql.functions import current_timestamp, col, lower, udf

#Configure logging.
logging.basicConfig(level=logging.INFO)
#set the logging level to INFO
logger = logging.getLogger("tmi_billingestion_dev_sa_provider_file_generation")

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
        
def get_df_db_data_with_query(read_db_sql, secrets):
    try:
        jdbc_url = f'jdbc:mysql://{secrets["host"]}:{secrets["port"]}/{secrets["dbClusterIdentifier"]}'
        read_db_sql = f"""({read_db_sql}) AS db_vw"""
        logger.info(f"read_db_sql => {read_db_sql}")
        df_db_res = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", read_db_sql) \
             .option("user", secrets["username"]).option("password", secrets["password"]).load()
        if df_db_res.count() == 0:
            logger.info(f"No matching records found for input read db sql!!!")
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
     process="Process : Provider File Generation"
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


def rename_s3_file(bucket_name,sa_file_path,new_file_name):
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=sa_file_path)
    if 'Contents' in response and response['Contents']:
        files = response['Contents']
        files_sorted = sorted(files, key=lambda x: x['LastModified'], reverse=True)
        target_file = files_sorted[0]["Key"]
        logger.info(f'Target_FILE : {target_file}')
        if target_file:
            cst=pytz.timezone('America/Chicago')
            logger.info(f"New File with CST timezone: {datetime.now(cst).strftime('%Y%m%d_%H%M%S')}")
            new_file_name = new_file_name.replace('<date_format>',datetime.now(cst).strftime('%Y%m%d_%H%M%S'))
            logger.info('new_file_name',new_file_name)
            new_file_rename=sa_file_path+new_file_name
            logger.info('new file name after rename',new_file_rename)
            copy_source = {'Bucket': bucket_name, 'Key': target_file}
            s3_client.copy_object(CopySource=copy_source, Bucket=bucket_name, Key=new_file_rename)
            s3_client.delete_object(Bucket=bucket_name, Key=target_file)
            logger.info(f"Renamed File {target_file} to {new_file_rename}")
    return True

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
    db_sp_params = job_id, 'Glue-ETL-Provider Feed', 'Glue-ETL-Provider Feed',0
    logger.info(f"db_sp_params => {db_sp_params}")
    dbStoredProcedure = "txm_bitx_provider_logs.insert_provider_logs"
    logger.info(f"dbStoredProcedure => {dbStoredProcedure}")
    batch_id = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, 3)
    logger.info(f"batch_id => {batch_id}")
    
    #Reading master configuration file
    AWSGlueDataCatalog_SA_master = glueContext.create_dynamic_frame.from_catalog(database="bitx_sa_mappings", table_name="sa_provider_mapping_master", transformation_ctx="AWSGlueDataCatalog_SA_master")
    AWSGlueDataCatalog_SA_master.printSchema()
    AWSGlueDataCatalog_SA_master.show()
    master_config_df = AWSGlueDataCatalog_SA_master.toDF()
    master_config_df.show()
    
    AWSGlueDataCatalog_SA_provider_lkp = glueContext.create_dynamic_frame.from_catalog(database="bitx_static_config", table_name="provider_type_code_lookup", transformation_ctx="AWSGlueDataCatalog_SA_provider_lkp")
    AWSGlueDataCatalog_SA_provider_lkp.printSchema()
    AWSGlueDataCatalog_SA_provider_lkp.show()
    provider_lkp_config_df = AWSGlueDataCatalog_SA_provider_lkp.toDF()
    provider_lkp_config_df.cache()
    provider_lkp_config_df.show()
    provider_lkp_config_df.createOrReplaceTempView("provider_lkp_config_vw")
    
    
    AWSGlueDataCatalog_SA_provider_iitc_lkp = glueContext.create_dynamic_frame.from_catalog(database="bitx_static_config", table_name="image_invoice_type_control", transformation_ctx="AWSGlueDataCatalog_SA_provider_iitc_lkp")
    AWSGlueDataCatalog_SA_provider_iitc_lkp.printSchema()
    AWSGlueDataCatalog_SA_provider_iitc_lkp.show()
    provider_iitc_lkp_config_df = AWSGlueDataCatalog_SA_provider_iitc_lkp.toDF()
    provider_iitc_lkp_config_df.cache()
    provider_iitc_lkp_config_df.show()
    provider_iitc_lkp_config_df.createOrReplaceTempView("provider_iitc_lkp_config_vw")
    
    read_db_provider_lkp_sql = f"""SELECT provider_number
                                          ,bill_item_type
                                          ,rendering_provider_license
                                          ,LEFT(TRIM(UPPER(rendering_provider_license)),3) AS prefix_3
                                          ,LEFT(TRIM(UPPER(rendering_provider_license)),2) AS prefix_2
                                    FROM txm_bitx_provider.provider 
                                    WHERE bill_item_type IS NOT  NULL
                                    AND delivered_to_sa IN ('NEW','UPDATED')
                                    AND (provider_type_code IS NULL OR TRIM(provider_type_code)='')"""
    
    logger.info('sql for rendering license provider type code-->',read_db_provider_lkp_sql)
    provider_type_lkp_df = get_df_db_data_with_query(read_db_provider_lkp_sql, secrets)
    
    provider_type_lkp_df.createOrReplaceTempView("provider_type_lkp_vw")
    
    provider_type_lkp_df.show(5,truncate=False)
    provider_type_lkp_df.cache()
    
    provider_type_join_df = spark.sql(f"""SELECT DISTINCT  pt.rendering_provider_license
                                                          ,pt.provider_number
                                                          ,pt.bill_item_type
                                                          ,COALESCE(plcd3.provider_type_code,plcd2.provider_type_code,
                                                          CASE WHEN TRIM(plcdii.batch_type)='H' THEN 'HP'
                                                               WHEN TRIM(plcdii.batch_type)='P' THEN 'RX'
                                                               WHEN TRIM(plcdii.batch_type)='T' THEN 'DS'
                                                               ELSE 'DR' END ) AS provider_type_code
                                                          FROM provider_type_lkp_vw pt
                                                          LEFT OUTER JOIN provider_lkp_config_vw plcd3 ON plcd3.provider_license_prefix = pt.prefix_3
                                                          LEFT OUTER JOIN provider_lkp_config_vw plcd2 ON plcd2.provider_license_prefix = pt.prefix_2
                                                          LEFT OUTER JOIN provider_iitc_lkp_config_vw plcdii ON plcdii.image_document_id= pt.bill_item_type""")
    
    logger.info("Provider Type based on rendering provider license")
    provider_type_join_df.show(10,truncate=False)
    
    update_main_table_name = f"txm_bitx_provider.provider"
    update_stage_table_name = f"{update_main_table_name}_stage"
    update_query = f"""
            UPDATE {update_main_table_name} main
            JOIN {update_stage_table_name} stage ON main.provider_number = stage.provider_number
            SET main.provider_type_code = stage.provider_type_code,update_user='Glue-ETL-Provider Feed',update_timestamp=NOW()"""
    logger.info('update query for ECI Provider Type-->',update_query)
    logger.info(f"Calling update_db_records function to update {update_main_table_name} in database...")
    update_db_records(provider_type_join_df, secrets, update_main_table_name, update_stage_table_name, update_query)
    
    iterator_m=master_config_df.rdd.toLocalIterator()
    datacatalog_schema_name=''
    datacatalog_validation_table_name=''
    df_table_name=''
    db_read_sql_select=''
    sa_file_path=''
    new_file_name=''
    FEIN=''
    status='SUCCESS'
    errorInfo=''
    errormessage=''
    provider_file_path=''
    for rowm in iterator_m:
        datacatalog_schema_name=str(rowm['datacatalog_schema_name'])
        datacatalog_validation_table_name=str(rowm['datacatalog_provider_mapping_table_name'])
        df_table_name=str(rowm['df_table_name'])
        db_read_sql_select=str(rowm['db_read_sql_select'])
        sa_file_path=str(rowm['s3_file_prefix'])
        new_file_name=str(rowm['outbound_file_name'])
        FEIN=str(rowm['FEIN'])
    
    read_sql=db_read_sql_select
    logger.info('sql from master config-->',read_sql)
    provider_list_df = get_df_db_data_with_query(read_sql, secrets)
    provider_list_df.show(10,truncate=False)
    
    # Validating Provide Count and process for mapping file
    provider_count=provider_list_df.count()
    logger.info('Provider Records counts for file generation-->',provider_count)
    if provider_count > 0:
        logger.info('Data is available for Provider Mapping !')
        provider_list_df = provider_list_df.cache()
        logger.info('FEIN',FEIN)
        provider_list_pu_rx_df=provider_list_df.filter(provider_list_df.vendor_tax_id==FEIN).filter(provider_list_df.provider_type_code=='RX')
        provider_list_pu_rx_df.cache()
        logger.info('Below are the RX records for FEIN-->',FEIN)
        provider_list_pu_rx_df.show(5,truncate=False)
        AWSGlueDataCatalog_MappingConfig = glueContext.create_dynamic_frame.from_catalog(database="bitx_sa_mappings", table_name="sa_mapping_provider_config", transformation_ctx="AWSGlueDataCatalog_MappingConfig")
        AWSGlueDataCatalog_MappingConfig.printSchema()
        config_df = AWSGlueDataCatalog_MappingConfig.toDF()
        config_df.show(5,truncate=False)
        config_df_pv=config_df.filter(config_df.record_type=='PV')
        config_df_pv.show(5,truncate=False)
        config_df_pu=config_df.filter(config_df.record_type=='PU')
        config_df_pu.show(5,truncate=False)
        config_df_hd=config_df.filter(config_df.record_type=='HD')
        config_df_hd.show(5,truncate=False)
        config_df_tl=config_df.filter(config_df.record_type=='TL')
        config_df_tl.show(5,truncate=False)
        config_df_pu_rx=config_df.filter(config_df.record_type=='PU_RX')
        config_df_pu_rx.show(5,truncate=False)
        iterator_pv=config_df_pv.orderBy(['field_order']).rdd.toLocalIterator()
        iterator_pu=config_df_pu.orderBy(['field_order']).rdd.toLocalIterator()
        iterator_hd=config_df_hd.orderBy(['field_order']).rdd.toLocalIterator()
        iterator_tl=config_df_tl.orderBy(['field_order']).rdd.toLocalIterator()
        iterator_pu_rx=config_df_pu_rx.orderBy(['field_order']).rdd.toLocalIterator()
        logics_pv=[]
        logics_pu=[]
        logics_hd=[]
        logics_tl=[]
        logics_pu_rx=[]
    
        for rowv in iterator_pv:
            logics_pv.append(rowv['sql_mapping_logic'])
        logger.info("Count of PV Elements from Mapping",len(logics_pv))
    
        for rowu in iterator_pu:
            logics_pu.append(rowu['sql_mapping_logic'])
        logger.info("Count of PU Elements from Mapping",len(logics_pu))
        
        for rowu_rx in iterator_pu_rx:
            logics_pu_rx.append(rowu_rx['sql_mapping_logic'])
        logger.info("Count of PU_RX Elements from Mapping",len(logics_pu_rx))
    
        for rowh in iterator_hd:
            logics_hd.append(rowh['sql_mapping_logic'])    
        logger.info("Count of Header Elements from Mapping",len(logics_hd))
        
        for rowt in iterator_tl:
            logics_tl.append(rowt['sql_mapping_logic'])    
        logger.info("Count of Trailer End Elements from Mapping",len(logics_tl))
        
    
        final_query_hd=','.join(logics_hd)
        logger.info("Header Query-->",final_query_hd)   
            
        final_query_pv=','.join(logics_pv)
        logger.info("PV Query-->",final_query_pv)
        
        final_query_pu=','.join(logics_pu)
        logger.info("PU Query-->",final_query_pu)
        
        final_query_pu_rx=','.join(logics_pu_rx)
        logger.info("PU_RX Query-->",final_query_pu_rx)
        
        final_query_tl=','.join(logics_tl)
        pv_count=str(provider_count)
        config_df_pu_rx_cnt=provider_list_pu_rx_df.count()
        pu_rx_count=str(provider_count+config_df_pu_rx_cnt)
        final_query_tl=final_query_tl.replace('CNT_PV',pv_count).replace('CNT_PU',pu_rx_count)
        logger.info("TL Query-->",final_query_tl)
        
        provider_list_df.createOrReplaceTempView(df_table_name)
        provider_list_pu_rx_df.createOrReplaceTempView("provider_pu_rx")
        
        provider_list_df.printSchema()
        provider_list_df.show(5, truncate=False)
        provider_list_count = provider_list_df.count()
        logger.info(f"Provider list Record Count => {provider_list_count}")
        HD_df=spark.sql("select Concat("+final_query_hd+") AS final,0 as provider_number,1 as seq")
        HD_df.printSchema()
        logger.info('Sample data below for Header')
        HD_df.show(5, truncate=False)
        HD_df.printSchema()
        PV_df=spark.sql("select Concat("+final_query_pv+") AS final,CAST("+logics_pv[2]+" AS BIGINT) as provider_number,2 as seq from "+df_table_name)
        PV_df.printSchema()
        logger.info('Sample data below for PV')
        PV_df.show(5, truncate=False)
        PV_df.printSchema()
        PU_df=spark.sql("select Concat("+final_query_pu+") AS final,CAST("+logics_pu[1]+" AS BIGINT) as provider_number,3 as seq from "+df_table_name)
        PU_df.printSchema()
        logger.info('Sample data below for PU')
        PU_df.show(5, truncate=False)
        PU_df_rx=spark.sql("select Concat("+final_query_pu_rx+") AS final,CAST("+logics_pu_rx[1]+" AS BIGINT) as provider_number,4 as seq from provider_pu_rx")
        PU_df_rx.printSchema()
        logger.info('Sample data below for PU_RX')
        PU_df_rx.show(5, truncate=False)
        TL_df=spark.sql("select Concat("+final_query_tl+") AS final,214748364712345 as provider_number,5 as seq")
        TL_df.printSchema()
        TL_df.show(1,truncate=False)
        provider_sa_df=PU_df.union(PV_df).union(HD_df).union(TL_df).union(PU_df_rx)
        provider_sa_df=provider_sa_df.orderBy(["provider_number", "seq"], ascending=[True, True])
        provider_sa_df.show(10,truncate=False)
        provider_number_distinct_df=provider_sa_df.select('provider_number').distinct()
        provider_sa_df=provider_sa_df.drop(provider_sa_df.provider_number).drop(provider_sa_df.seq)
        provider_sa_df.printSchema()
        provider_file_path='s3://'+bucket_name+'/'+sa_file_path
        provider_sa_df.coalesce(1).write.option('header',False).option('lineSep','\r\n').mode('append').format('text').save(provider_file_path) 
        logger.info('File is uploaded into S3Path Successfully!')
        logger.info('bucket_name',bucket_name)
        logger.info('prefix',sa_file_path)
        file_response=rename_s3_file(bucket_name,sa_file_path,new_file_name)
        if file_response:
            update_main_table_name = f"txm_bitx_provider.provider"
            logger.info(f"update_maain_table_name => {update_main_table_name}")
            update_stage_table_name = f"{update_main_table_name}_stage"
            logger.info(f"update_stage_table_name => {update_stage_table_name}")
            update_query = f"""
            UPDATE {update_main_table_name} main
            JOIN {update_stage_table_name} stage ON main.provider_number = stage.provider_number
            SET main.delivered_to_sa = 'DELIVERED',update_user='Glue-ETL-Provider Feed',update_timestamp=NOW(6)
            """
            logger.info('update query-->',update_query)
            logger.info(f"Calling update_db_records function to update {update_main_table_name} in database...")
            update_db_records(provider_number_distinct_df, secrets, update_main_table_name, update_stage_table_name, update_query)
    else:
        logger.info('No Provider Records to Process!')
except Exception as e:
    logger.error(e)
    errorInfo='Aborted Provider File Generation'
    errormessage=str(e)[:500]
    log_and_email(env_profile,errorInfo,errormessage,"FATAL",sns_topic_arn)
    status="FAILED"
    raise Exception("Aborted File Generation !!")
finally:
    db_sp_params = batch_id,status, 'Glue', errorInfo, errormessage, 'Glue-ETL-Provider Feed', 'Glue-ETL-Provider Feed'
    logger.info(f"db_sp_params => {db_sp_params}")
    dbStoredProcedure = "txm_bitx_provider_logs.update_provider_log_status"
    logger.info(f"dbStoredProcedure => {dbStoredProcedure}")
    res = call_db_stored_procedure(secrets, dbStoredProcedure, db_sp_params, None)
job.commit()