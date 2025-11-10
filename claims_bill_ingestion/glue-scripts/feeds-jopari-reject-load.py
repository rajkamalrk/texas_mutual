import sys
import logging
import json
import base64
import boto3
import pytz
import time
from botocore.exceptions import ClientError
from botocore.client import Config
from awsglue.job import Job
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
import pymysql
from datetime import datetime, date
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import StructType, StringType
from glue_commons import bitx_glue_common as lib
from glue_commons import bitx_feeds_glue_common as feeds_lib

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("claimant-billingestion-feeds-jopari-reject-load-job")

## @params: [claimant-billingestion-feeds-jopari-reject-load-job]
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'env_profile',
    'feed_type',
    'provider',
    'db_secret_key',
    'sns_topic_arn',
    's3_bucket',
    'datacatalog_schema_name',
    'datacatalog_mapping_master',
    'file_path'
])

# Create GlueContext, SparkContext, and SparkSession
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
logger.info("Glue job initialized successfully...")
job_id = args['JOB_RUN_ID']
logger.info(f"job_id => {job_id}")
job_name = args['JOB_NAME']
logger.info(f"job_name => {job_name}")

# Extract job parameters
env_profile = args['env_profile']
feed_type = args['feed_type']
provider = args['provider']
batch_id = None
db_secret_key = args['db_secret_key']
sns_topic_arn = args['sns_topic_arn']
s3_bucket = args['s3_bucket']
datacatalog_schema_name = args['datacatalog_schema_name']
datacatalog_mapping_master = args['datacatalog_mapping_master']
file_path=args['file_path']

# Generic methods are now imported from bitx_glue_common library
# Feeds-specific methods are now imported from bitx_feeds_glue_common library   
    
# Initializations
mode = "append"
status = "IN PROGRESS"
step_id = None
database='txm_bitx'

try:
    logger.info(f"Processing STARTED for Jopari Reject Feed...")
    logger.info(f"db_secret_key => {db_secret_key}")
    secrets = lib.get_secret(db_secret_key, 'us-east-1')
    batch_id=feeds_lib.create_feeds_batch(secrets, feed_type, provider, 'None', file_path, 'Jopari Reject Feed')
    # Insert step log and get step_id
    step_id = feeds_lib.feeds_step_logging(secrets, batch_id, step_id, job_name, job_id, 'IN PROGRESS', 'Glue - Feeds Jopari Reject')
    logger.info(f"Step record inserted with step_id => {step_id}")
    
    AWSGlueDataCatalog_JR_master = glueContext.create_dynamic_frame.from_catalog(database=datacatalog_schema_name, table_name=datacatalog_mapping_master, transformation_ctx="AWSGlueDataCatalog_JR_master")
    AWSGlueDataCatalog_JR_master.printSchema()
    AWSGlueDataCatalog_JR_master.show()
    master_config_df = AWSGlueDataCatalog_JR_master.toDF()
    master_config_df.show()
    
    iterator_m=master_config_df.rdd.toLocalIterator()
    datacatalog_static_schema_name=None
    datacatalog_mapping_table_name=None
    datacatalogs_static_table_name=None
    db_read_sql_select=None
    outbound_file_name=None
    s3_file_prefix=None
    df_table_name=None
    new_file_name=None
    
    for rowm in iterator_m:
        datacatalog_static_schema_name=str(rowm['datacatalog_static_schema_name'])
        datacatalog_mapping_table_name=str(rowm['datacatalog_mapping_table_name'])
        datacatalogs_static_table_name=str(rowm['datacatalogs_static_table_name'])
        db_read_sql_select=str(rowm['db_read_sql_select'])
        new_file_name=str(rowm['outbound_file_name'])
        s3_file_prefix=str(rowm['s3_file_prefix'])
        df_table_name=str(rowm['df_table_name'])

    read_db_sql=db_read_sql_select
    logger.info('sql from master config-->',read_db_sql)
    jopari_list_df = lib.get_df_db_data_with_query(spark,read_db_sql, secrets)
    jopari_list_df.show(10,truncate=False) 
    jopari_reject_count=jopari_list_df.count()
    logger.info('Provider Records counts for file generation-->',jopari_reject_count)
    if jopari_reject_count > 0:
        logger.info(f'Data is available for jopari_reject_count !=>{jopari_reject_count}')
        jopari_list_df = jopari_list_df.cache()
        input_df=jopari_list_df.select(col('status_type'),col('invoice_number'))
        indicated_attachment_missing_df=input_df.filter(input_df.status_type=='INDICATED_ATTACHMENT_MISSING')
        claim_match_manual_df=input_df.filter(input_df.status_type=='CLAIM_MATCH_MANUAL')
        vendor_match_manual_df=input_df.filter(input_df.status_type=='VENDOR_MATCH_MANUAL')
        adjudication_df=input_df.filter(input_df.status_type=='ADJUDICATION')
        #static table config
        AWSGlueDataCatalog_StaticConfig = glueContext.create_dynamic_frame.from_catalog(database=datacatalog_static_schema_name, table_name=datacatalogs_static_table_name, transformation_ctx="AWSGlueDataCatalog_StaticConfig")
        AWSGlueDataCatalog_StaticConfig.printSchema()
        static_df = AWSGlueDataCatalog_StaticConfig.toDF()
        static_df.show()
        trans_to = str(static_df.select("transto").first()[0])
        logger.info(f"trans_to=>{trans_to}")
        AWSGlueDataCatalog_MappingConfig = glueContext.create_dynamic_frame.from_catalog(database=datacatalog_schema_name, table_name=datacatalog_mapping_table_name, transformation_ctx="AWSGlueDataCatalog_MappingConfig")
        AWSGlueDataCatalog_MappingConfig.printSchema()
        config_df = AWSGlueDataCatalog_MappingConfig.toDF()
        config_df.show(5,truncate=False)
        config_df.show(5,truncate=False)
        config_df_dt=config_df.filter(config_df.record_type=='DT')
        config_df_dt.show(5,truncate=False)
        config_df_hd=config_df.filter(config_df.record_type=='HD')
        config_df_hd.show(5,truncate=False)
        config_df_tl=config_df.filter(config_df.record_type=='TL')
        config_df_tl.show(5,truncate=False)
        iterator_hd=config_df_hd.orderBy(['field_order']).rdd.toLocalIterator()
        iterator_dtl=config_df_dt.orderBy(['field_order']).rdd.toLocalIterator()
        iterator_tl=config_df_tl.orderBy(['field_order']).rdd.toLocalIterator()
        logics_hd=[]
        logics_dtl=[]
        logics_tl=[]
        for rowhd in iterator_hd:
            logics_hd.append(rowhd['sql_mapping_logic'])
        logger.info("Count of header Elements from Mapping",len(logics_hd))
    
        for rowdtl in iterator_dtl:
            logics_dtl.append(rowdtl['sql_mapping_logic'])
        logger.info("Count of detail Elements from Mapping",len(logics_dtl))

        for rowtl in iterator_tl:
            logics_tl.append(rowtl['sql_mapping_logic'])
        logger.info("Count of detail Elements from Mapping",len(logics_tl))

        final_query_hd=','.join(logics_hd)
        final_query_hd=final_query_hd.replace('trans_to',trans_to)
        logger.info("Header Query-->",final_query_hd) 
        

        final_query_dtl=','.join(logics_dtl)
        logger.info("Dtl Query-->",final_query_dtl)

        final_query_tl=','.join(logics_tl)
        logger.info("tl Query-->",final_query_tl)

        hd_df=spark.sql("select Concat("+final_query_hd+") AS final,1 as seq")
        hd_df.printSchema()
        logger.info('Sample data below for Header')
        hd_df.show(5, truncate=False)
        hd_df.printSchema()
        
        jopari_list_df.createOrReplaceTempView(df_table_name)
        dtl_df=spark.sql("select Concat("+final_query_dtl+") AS final, 2 as seq from "+df_table_name)
        dtl_df.printSchema()
        logger.info('Sample data below for detail')
        dtl_df.show(5, truncate=False)
        dtl_df.printSchema()
        final_query_tl=final_query_tl.replace('invoice_count',str(jopari_reject_count))
        tl_df=spark.sql("select Concat("+final_query_tl+") AS final,3 as seq")
        tl_df.printSchema()
        logger.info('Sample data below for trailer')
        tl_df.show(5, truncate=False)
        tl_df.printSchema()
        jopari_reject_df=hd_df.union(dtl_df).union(tl_df)
        jopari_reject_df=jopari_reject_df.orderBy([ "seq"], ascending=[True])
        jopari_reject_df.show(10,truncate=False)
        jopari_reject_df=jopari_reject_df.drop(jopari_reject_df.seq)
        jopari_reject_df.printSchema()
        jopari_reject_file_path='s3://'+s3_bucket+'/'+s3_file_prefix
        jopari_reject_df.coalesce(1).write.option('header',False).option('lineSep','\r\n').mode('append').format('text').save(jopari_reject_file_path) 
        logger.info('File is uploaded into S3Path Successfully!')
        file_response=lib.rename_s3_file(s3_bucket,s3_file_prefix,new_file_name)
        if file_response:
            status_txm_inv='NOTIFIED_REJECTED'
            if indicated_attachment_missing_df.count() > 0:
                status_type='INDICATED_ATTACHMENT_MISSING'
                lib.update_invoice_status(indicated_attachment_missing_df, secrets, status_type, status_txm_inv, 'jopari_reject', batch_id, update_user="Glue - ETL - Jopari Reject")
                logger.info(f"Below Status_type=>{status_type} got updated =>{status_txm_inv}")
            if claim_match_manual_df.count() > 0:
                status_type='CLAIM_MATCH_MANUAL'
                lib.update_invoice_status(claim_match_manual_df, secrets, status_type, status_txm_inv, 'jopari_reject', batch_id, update_user="Glue - ETL - Jopari Reject")
                logger.info(f"Below Status_type=>{status_type} got updated =>{status_txm_inv}")
            if adjudication_df.count() > 0:
                status_type='ADJUDICATION'
                lib.update_invoice_status(adjudication_df, secrets, status_type, status_txm_inv, 'jopari_reject', batch_id, update_user="Glue - ETL - Jopari Reject")
                logger.info(f"Below Status_type=>{status_type} got updated =>{status_txm_inv}")
            if vendor_match_manual_df.count() > 0:
                status_type='VENDOR_MATCH_MANUAL'
                lib.update_invoice_status(vendor_match_manual_df, secrets, status_type, status_txm_inv, 'jopari_reject', batch_id, update_user="Glue - ETL - Jopari Reject")
                logger.info(f"Below Status_type=>{status_type} got updated =>{status_txm_inv}")
    else:
        logger.info(f"No Records to Process {feed_type}!")    

    # Set load timestamp
    v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    logger.info(f"v_load_timestamp => {v_load_timestamp}")
    status='COMPLETED'
    feeds_lib.feeds_step_logging(secrets, batch_id, step_id, job_name, job_id, status, 'Glue - Feeds Jopari Reject')
    logger.info(f"[SUCCESS] Successfully updated step_id {step_id} to status {status}.")
    feeds_lib.update_feeds_batch_status(secrets, batch_id, status, 'Glue - Feeds Jopari Reject')
    logger.info(f"Processing COMPLETED for {feed_type} Jopari Reject Feed Load!!!")
    
except Exception as e:
    status = "FAILED"
    logger.error(f"Error in Jopari Reject Feed: {e}")
    error_info = "Failed in Load"
    error_message = str(e)
    feeds_lib.feeds_error_logging(secrets, batch_id, step_id, None, status, job_name, job_id, 'Jopari Reject Feed INVOICE GLUE FAILED', error_info, error_message, 'Glue - Jopari Reject Feed', True)
    logger.info(f"[FAILURE] Logged failure for batch_id {batch_id} and step_id {step_id}")
    response = feeds_lib.log_and_email(env_profile, provider, feed_type, 'N', batch_id, '', '', '', error_info, error_message, "FATAL", sns_topic_arn, "Glue-ETL-Jopari Reject Load")
    logger.info(f"Email sent successfully!message_id:{response['MessageId']}")
    raise e

try:
    # Commit the job
    job.commit()
    logger.info("Job committed successfully")
except Exception as e:
    logger.error(f"Error committing the job:{e}")
    raise