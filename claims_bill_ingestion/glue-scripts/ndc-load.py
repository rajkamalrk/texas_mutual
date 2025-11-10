import sys
import logging
import json
import base64
import boto3
import pytz
import pymysql
import time
import builtins
from datetime import datetime
from botocore.exceptions import ClientError
from botocore.client import Config
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit, col, lower, udf, when, expr, concat, count, round,trim,sha2,concat_ws
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from glue_commons import bitx_glue_common as lib


#Configure logging.
logging.basicConfig(level=logging.INFO)
#set the logging level to INFO
logger = logging.getLogger("claimant-billingestion-NDC-load-job")

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, [
    's3_bucket',
    's3_input_files_prefix',
    'datacatalog_schema_name',
    'datacatalog_config_table_name',
    'db_secret_key',
    'file_create_date',
    'batch_id',
    'env_profile',
    'sns_topic_arn'
    ])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job=Job(glueContext)
logger.info("Glue job initialized successfully...")
job_id = args['JOB_RUN_ID']
job_name= "claimant-billingestion-NDC-load-job"
logger.info(f"jobid-{job_id}")
s3_bucket=args['s3_bucket'].strip()
datacatalog_schema_name=args['datacatalog_schema_name'].strip()
datacatalog_config_table_name=args['datacatalog_config_table_name'].strip()
db_secret_key=args['db_secret_key'].strip()
batch_id=args['batch_id'].strip()
file_create_date=args['file_create_date'].strip()
env_profile=args['env_profile'].strip()
sns_topic_arn=args['sns_topic_arn'].strip()
s3_input_files_prefix=args['s3_input_files_prefix'].strip()
#intitilizations
mode = "append"
status="IN PROGRESS"
step_id=0
file_name=''
latest_summary_id=0
logging_table="txm_ndc_logs.batch_ndc_step_details"
error_logging_table="txm_ndc_logs.batch_ndc_error_details"
spark.conf.set("spark.sql.session.timeZone", "UTC")


        
def ndc_load_insert(secrets,logging_table,error_logging_table,status,batch_id,file_create_date,job_name,job_id,load_type,error_code,error_info,error_message):
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
        v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        logger.info(f"Logging Audit Entry info into {logging_table}...")
        cursor.execute('SET LOCAL net_read_timeout = 300')
        step_id=0
        if load_type=='Audit':
            sql = f"""INSERT INTO {logging_table} (batch_id,file_create_date,job_type,job_name,job_id,status,start_datetime,end_datetime,create_user,create_timestamp,update_user,update_timestamp) VALUES (%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s)"""
            val = (batch_id,file_create_date,'GLUE',job_name,job_id,status,v_load_timestamp,None,'Glue-ETL-NDC Load',v_load_timestamp,'Glue-ETL-NDC Load',v_load_timestamp)
            cursor.execute(sql, val)
            conn.commit()
        step_id_query=f"""SELECT Max(step_id) FROM {logging_table} WHERE job_type='GLUE' """
        cursor.execute(step_id_query)
        db_step_id = cursor.fetchone()
        for record in db_step_id:
            step_id=record
            print(record)
        logger.info(f"Query executed successfully with step_id {step_id}")
        if load_type=='Error':
            sql = f"""INSERT INTO {error_logging_table} (batch_id,step_id,error_code,error_info,error_message,create_user,create_timestamp,update_user,update_timestamp)
               VALUES (%s, %s,%s, %s,%s, %s,%s, %s,%s)"""
            logger.info(f"update query =>{sql}")
            logger.info(f" step_id for update query =>{step_id}")
            val = (batch_id,step_id,error_code,error_info,error_message,'Glue-ETL-NDC Load',v_load_timestamp,'Glue-ETL-NDC Load',v_load_timestamp)
            cursor.execute(sql, val)
            conn.commit()
    except Exception as e:
        logger.error("Insertion Failed in DB: %s", str(e))
        raise e
    finally:
        conn.close()
    return step_id   

def ndc_load_update(secrets,logging_table,step_id,status):
    v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
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
        sql = f"""UPDATE {logging_table} SET status='{status}',end_datetime='{v_load_timestamp}',update_timestamp='{v_load_timestamp}' WHERE step_id={step_id} """
        logger.info(f"update query=>{sql}")
        cursor.execute(sql)
        conn.commit()
    except Exception as e:
        logger.error("Failed updates: %s", str(e))
        raise e
    finally:
        conn.close()
    return 'Updated Successfully!'      
 
def log_and_email(env_profile,errorInfo,file_name,trading_partner,errormessage,severity,subject,process,sns_topic_arn):
     """logging error and sending error email"""
     logger.info("inside log email method")
     sns_client=boto3.client('sns',region_name='us-east-1')
     cst=pytz.timezone('America/Chicago')
     timestamp=datetime.now(cst).strftime('%Y-%m-%d %H:%M:%S %Z%z')
     trading_partner=trading_partner.upper()
     #create log message
     errInfo=f"Error  : {errorInfo}"
     time=f"Timestamp : {timestamp}"
     file_name=f": {file_name}"
     errLog=f"Validation error message name : {errormessage}"
     log_message=f"{process}\n{errInfo}\n{file_name}\n{time}"
     logger.error(log_message)
     subject=f"{env_profile} - {trading_partner} - {errorInfo} -{severity}"
     message_body = (
            f"Process: MEDISPAN - Load M25 Data files\n"
            f"File Name: {file_name}\n"
            f"Error Message: {errormessage}\n"
            f"Timestamp : {datetime.now(cst).strftime('%Y-%m-%d %H:%M:%S.%f')}"
        )
     response=sns_client.publish(
         TopicArn=sns_topic_arn,
         Subject=subject,
         Message=message_body
     )
     return response     
try:
    logger.info(f"Processing STARTED for NDC...")
    logger.info(f"db_secret_key => {db_secret_key}")
    secrets = lib.get_secret(db_secret_key, 'us-east-1')
    step_id=ndc_load_insert(secrets, logging_table,error_logging_table,status,batch_id,file_create_date,job_name,job_id,'Audit',None,None,None)
    logger.info(f"Audit record inserted with step_id => {step_id}")
    AWSGlueDataCatalog_NDC_Master_Config = glueContext.create_dynamic_frame.from_catalog(database=datacatalog_schema_name, table_name=datacatalog_config_table_name, transformation_ctx="AWSGlueDataCatalog_NDC_Master_Config")
    AWSGlueDataCatalog_NDC_Master_Config.printSchema()
    AWSGlueDataCatalog_NDC_Master_Config.show()
    config_master_df = AWSGlueDataCatalog_NDC_Master_Config.toDF()
    config_rec = config_master_df.first()
    
    AWSGlueDataCatalog_NDC_Config = glueContext.create_dynamic_frame.from_catalog(database=config_rec["datacatalog_schema_name"], table_name=config_rec["datacatalog_table_name"], transformation_ctx="AWSGlueDataCatalog_NDC_Config")
    AWSGlueDataCatalog_NDC_Config.printSchema()
    AWSGlueDataCatalog_NDC_Config.show()
    ndc_config_df = AWSGlueDataCatalog_NDC_Config.toDF()
    ndc_config_df.show(3, truncate=False)
    recordCount = ndc_config_df.count()
    logger.info(f"Config Record Count => {recordCount}")
    ndc_config_itr = ndc_config_df.orderBy(['order_seq_no']).rdd.toLocalIterator()
    for row in ndc_config_itr:
        logger.info(f"Processing started for row: {row}")
        if row["record_type"]=='M25_Summary_load':
            s3_path=f"s3://{s3_bucket}/{s3_input_files_prefix}/M25SUM"
            logger.info(f"s3_path => {s3_path}")
            init_df=spark.read.text(s3_path).toDF("ndc_data_str")
            init_df.printSchema()
            init_df.show(5,truncate=False)
            v_load_timestamp = None
            v_filter_id_col_value = None
            logger.info("Summary Load block")
            init_df.createOrReplaceTempView(row["df_table_name"])
            record_type_data_transform_df = spark.sql(row["transform_sql_select"])
            record_type_data_transform_df.show(5,truncate=False)
            recordCount = record_type_data_transform_df.count()
            logger.info(f"Adding ETL processing columns...")
            v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            logger.info(f"v_load_timestamp => {v_load_timestamp}")
            record_type_data_transform_df = record_type_data_transform_df \
                .withColumn("create_user", lit("Glue ETL - NDC load")) \
                .withColumn("update_user", lit("Glue ETL - NDC load")) \
                .withColumn("create_timestamp", lit(v_load_timestamp)) \
                .withColumn("update_timestamp", lit(v_load_timestamp))
            record_type_data_transform_df.printSchema()
            lib.load_data_to_rds(record_type_data_transform_df, row["db_table_name"], secrets, mode)
            status="COMPLETED"
        if row["record_type"]=='M25_load' :
            s3_path=f"s3://{s3_bucket}/{s3_input_files_prefix}/M25"
            file_name=f"{s3_input_files_prefix}M25"
            logger.info(f"s3_path => {s3_path}")
            init_df=spark.read.text(s3_path).toDF("ndc_data_str")
            init_df.printSchema()
            init_df.show(5,truncate=False)
            v_load_timestamp = None
            v_filter_id_col_value = None
            time.sleep(30)
            validation_sql=row['validation']
            validation_sql=validation_sql.replace('<issue_date>',file_create_date)
            ndc_summary_id=0
            logger.info(f"validation query=>{validation_sql}")
            validation_df=lib.get_df_db_data_with_query(spark,validation_sql, secrets)
            validation_df.show(5,truncate=False)
            validation_df_itr=validation_df.rdd.toLocalIterator()
            for record in validation_df_itr:
                if record['validation_result']!='PASS':
                    logger.info(f"Validation Failed with=> {record['validation_result']}")
                    subject='MEDISPAN-Data Load Validation Failed'
                    process=f"Validation Failed with Reason-{record['validation_result']}"
                    response=log_and_email(env_profile,'MEDISPAN-Data Load Validation Failed',file_name,'MEDISPAN',record['validation_result'],"FATAL",subject,process,sns_topic_arn)
                    logger.info(f"Response for email validation =>{response}")
                else :
                    read_db_sql = f"""SELECT ndc_code
                                        ,ndc_name
                                        ,ndc_name_extension
                                        ,ndc_item_status
                                        ,ndc_unit_of_measure
                                        ,ndc_status_change_date
                                        ,ndc_strength
                                        FROM txm_ndc.ndc_data"""
                    ndc_source_df = lib.get_df_db_data_with_query(spark,read_db_sql, secrets)
                    ndc_source_df.registerTempTable("tgt_tbl")
                    ndc_source_df.show(5,truncate=False)
                    read_db_sql_summary = f"""SELECT MAX(ndc_summary_id) AS ndc_summary_id
                                              FROM txm_ndc.ndc_summary_data"""
                    ndc_summary_id_df = lib.get_df_db_data_with_query(spark,read_db_sql_summary, secrets)
                    ndc_summary_id_itr=ndc_summary_id_df.rdd.toLocalIterator()
                    for record in ndc_summary_id_itr:
                        ndc_summary_id=record['ndc_summary_id']
                        logger.info(f"ndc_summary_id=>{ndc_summary_id}")
                    init_df.createOrReplaceTempView(row["df_table_name"])
                    record_type_data_transform_df = spark.sql(row["transform_sql_select"])
                    logger.info(f"Source Data before load")
                    record_type_data_transform_df.show(5,truncate=False)
                    record_type_data_transform_df.registerTempTable("src_tbl")
                    record_type_data_transform_df = spark.sql(row["cdc_retrieval"])
                    logger.info(f"CDC Data before load")
                    record_type_data_transform_df.printSchema()
                    record_type_data_transform_df.show(5, truncate=False)
                    recordCount = record_type_data_transform_df.count()
                    logger.info(f"Record Type Data Transform Record Count => {recordCount}")
                    logger.info(f"Adding ETL processing columns...")
                    v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                    logger.info(f"v_load_timestamp => {v_load_timestamp}")
                    record_type_data_transform_df = record_type_data_transform_df \
                        .withColumn("ndc_summary_id", lit(ndc_summary_id)) \
                        .withColumn("create_user", lit("Glue ETL - NDC load")) \
                        .withColumn("update_user", lit("Glue ETL - NDC load")) \
                        .withColumn("create_timestamp", lit(v_load_timestamp)) \
                        .withColumn("update_timestamp", lit(v_load_timestamp))
                    record_type_data_transform_df.printSchema()
                    cdc_df_inserts=record_type_data_transform_df.filter(record_type_data_transform_df.record_type=='Insert').drop(record_type_data_transform_df.record_type)
                    no_of_records_inserted=cdc_df_inserts.count()
                    logger.info(f"New Records for insertion {no_of_records_inserted}")
                    cdc_df_updates=record_type_data_transform_df.filter(record_type_data_transform_df.record_type=='Update').drop(record_type_data_transform_df.record_type)
                    no_of_records_updates=cdc_df_updates.count()
                    logger.info(f"Records for Updation {no_of_records_updates}")
                    if no_of_records_inserted > 0:
                        lib.load_data_to_rds(cdc_df_inserts, row["db_table_name"], secrets, mode)
                    else:
                        logger.info("No Records for Insertion!")
                    if no_of_records_updates > 0:
                        update_main_table_name = f"txm_ndc.ndc_data"
                        logger.info(f"update_main_table_name => {update_main_table_name}")
                        update_stage_table_name = f"{update_main_table_name}_stage"
                        logger.info(f"update_stage_table_name => {update_stage_table_name}")
                        update_query = f"""
                                    UPDATE {update_main_table_name} main
                                    JOIN {update_stage_table_name} stage ON main.ndc_code = stage.ndc_code 
                                    SET main.ndc_name=stage.ndc_name
                                    ,main.ndc_name_extension=stage.ndc_name_extension
                                    ,main.ndc_item_status=stage.ndc_item_status
                                    ,main.ndc_status_change_date=stage.ndc_status_change_date
                                    ,main.ndc_strength=stage.ndc_strength
                                    ,main.ndc_unit_of_measure=stage.ndc_unit_of_measure
                                    ,main.update_timestamp='{v_load_timestamp}'
                                    ,main.ndc_summary_id={ndc_summary_id}
                        """
                        lib.update_db_records(cdc_df_updates, secrets, update_main_table_name, update_stage_table_name, update_query,"txm_ndc")
                    else:
                        logger.info("No Records for Updation!")   
except Exception as e:
    status="ERROR"
    logger.error(f"Error committing the job:{e}")
    ndc_load_insert(secrets, logging_table,error_logging_table,status,batch_id,file_create_date,job_name,job_id,'Error','GLUE FAILED','Failed in load',str(e)[:100])
    subject="MEDISPAN NDC Load Failed"
    process= f"Failed For ETL"   
    response=log_and_email(env_profile,'Glue Failed',file_name,'MEDISPAN',str(e)[:100],"FATAL",subject,process,sns_topic_arn)
    raise 
finally:
    conn = boto3.client('s3')
    folder_name=s3_input_files_prefix.split('/')[-2]
    for key in conn.list_objects(Bucket=s3_bucket,Prefix=s3_input_files_prefix)['Contents']:
        logger.info(f"Copied key=>{key['Key']}")
        file_name=key['Key'].split('/')[-1]
        logger.info(f"filename=>{file_name}")
        archive_key = f"data/source/ndc/medispan/archive/ndc_code_files/{folder_name}/{file_name}"    
        logger.info(f"archivekey==>{archive_key}")
        response=lib.archive_file(bucketName=s3_bucket,archivekey=archive_key,sourceKey=key['Key'])
        logger.info(f"response=>{response}")
    ndc_load_update(secrets,logging_table,step_id,status)
job.commit()