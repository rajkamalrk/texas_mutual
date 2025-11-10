import sys
import logging
import json
import boto3
import pytz
from datetime import datetime
from botocore.exceptions import ClientError
from botocore.client import Config
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit, col, lower, udf,monotonically_increasing_id
from preauth_glue_commons import preauth_glue_common as preauth_lib
from pyspark.sql.types import StructType


#Configure logging.
logging.basicConfig(level=logging.INFO)
#set the logging level to INFO
logger = logging.getLogger("claimant-preauth-staging-load-job")

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, [ 'JOB_NAME',
    's3_bucket',
    'input_file_name',
    's3_input_files_prefix',
    'datacatalog_schema_name',
    'datacatalog_config_table_name',
    'db_secret_key',
    'batch_id',
    'env_profile',
    'sns_topic_arn',
    'load_category',
    'provider',
    'reprocess_flag',
    'step_function_info',
    'step_function_execution_id',
    'failed_batch_id'
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

s3_bucket=args['s3_bucket'].strip()
input_file_name=args['input_file_name'].strip()
datacatalog_schema_name=args['datacatalog_schema_name'].strip()
datacatalog_config_table_name=args['datacatalog_config_table_name'].strip()
db_secret_key=args['db_secret_key'].strip()
batch_id=args['batch_id'].strip()
env_profile=args['env_profile'].strip()
sns_topic_arn=args['sns_topic_arn'].strip()
s3_input_files_prefix=args['s3_input_files_prefix'].strip()
load_category=args['load_category'].strip()
provider = args['provider'].strip()
reprocess_flag = args['reprocess_flag'].strip()
failed_batch_id = int(args['failed_batch_id'].strip())
step_function_info = args.get('step_function_info')
step_function_execution_id = args.get('step_function_execution_id')
failed_step_id=None
updt_batch_id=batch_id
#intitilizations
mode = "append"
status="IN PROGRESS"
notes_file_header_id=0
logging_table="txm_preauth_logs.batch_preauth_step_details"
error_logging_table="txm_preauth_logs.batch_preauth_error_details"
#spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
claim_number=0 
unique_id=0
activity_name=''
final_string=[]
is_execution=True
is_reprocess_exec=False
reprocess_valid_list=['completed','completed_in_reprocess','skip_execution']
try:
    logger.info(f"Processing STARTED for Pre Auth...")
    logger.info(f"db_secret_key => {db_secret_key}")
    secrets = preauth_lib.get_secret(db_secret_key, 'us-east-1')
    step_id = preauth_lib.preauth_step_logging(secrets, batch_id, None, job_name, job_id, status, 'Glue-ETL-Pre-Auth Staging Load')
    logger.info(f"Step record inserted with step_id => {step_id}")

    if reprocess_flag.upper().strip()=='N' and batch_id:
       dup_chk=f""" SELECT count(*) as cnt FROM txm_preauth_staging.preauth_notes_file_header nfh 
                    INNER JOIN txm_preauth_staging.preauth_notes_header nh ON nh.notes_file_header_id=nfh.notes_file_header_id
                    INNER JOIN txm_preauth_staging.preauth_notes_detail nd ON nh.notes_header_id=nd.notes_header_id
                    WHERE batch_id={batch_id}  """
       dup_chkstatus=preauth_lib.run_sql_query(dup_chk, secrets)
       dup_chk_cnt= int(dup_chkstatus[0]['cnt'])
       if(dup_chk_cnt) > 0:
           is_execution=False
           raise Exception("Duplicate Batch!.Batch is already Processed.")
    
    
    if reprocess_flag.upper().strip()=='Y':
        validate_sql=f"""SELECT  m.batch_id,st.status,st.step_id,m.file_name,file_type,file_path FROM txm_preauth_logs.batch_preauth_step_details st
                     INNER JOIN   txm_preauth_logs.batch_preauth_master m ON m.batch_id=st.batch_id
                     WHERE m.batch_id={failed_batch_id}  AND job_name='{job_name}' """
        validation_stg_status=preauth_lib.run_sql_query(validate_sql, secrets)
        print(f"validation_stg_status => {validation_stg_status}")
        
        if validation_stg_status:
            load_category=validation_stg_status[0]['file_type'].lower()
            file_path=validation_stg_status[0]['file_path'].lower()
            failed_step_id=int(validation_stg_status[0]['step_id'])
            if load_category == 'letters':
                if isinstance(validation_stg_status, list):
                    if str(validation_stg_status[0]['status']).lower() in reprocess_valid_list :
                        print(f"Batch=> {failed_batch_id} with file name =>{validation_stg_status[0]['file_name']} is already processed succesfully in the last run.Terminating Reprocess!")
                        is_execution=False
                        reprocess_status='SKIP_EXECUTION'
                        logger.info('Updating through inbuilt method!')
                        preauth_lib.preauth_step_logging(secrets, batch_id,step_id, None, None, reprocess_status, 'Glue-ETL-Pre-Auth Staging Reprocess Load')
                    else:
                        s3_input_files_prefix=f"{s3_input_files_prefix.replace('preauth_code_files','error')}"
                        destination_prefix=f"{s3_input_files_prefix.replace('error','preauth_code_files')}"
                        s3_client = boto3.client("s3")
                        for key in s3_client.list_objects(Bucket=s3_bucket,Prefix=s3_input_files_prefix)['Contents']:
                            logger.info(f"Copied key=>{key['Key']}")
                            file_name=key['Key'].split('/')[-1]
                            logger.info(f"filename=>{file_name}")
                            archive_key = f"{destination_prefix}{file_name}"   
                            logger.info(f"archivekey==>{archive_key}")
                            response=preauth_lib.archive_file(bucketName=s3_bucket,archivekey=archive_key,sourceKey=key['Key'])
                            logger.info(f"response=>{response}")
                            s3_input_files_prefix=f"{destination_prefix}"
                            is_execution=True
                            is_reprocess_exec=True 
            elif load_category == 'notes':
                logger.info("Reprocess for Notes!")
                if isinstance(validation_stg_status, list):
                        if str(validation_stg_status[0]['status']).lower() in reprocess_valid_list:
                            print(f"Batch=> {failed_batch_id} with file name =>{validation_stg_status[0]['file_name']} is already processed succesfully in the last run.Terminating Reprocess!")
                            is_execution=False
                            reprocess_status='SKIP_EXECUTION'
                            preauth_lib.preauth_step_logging(secrets, batch_id,step_id, None, None, reprocess_status, 'Glue-ETL-Pre-Auth Staging Reprocess Load')
                        else:
                            s3_input_files_prefix=f"{s3_input_files_prefix.replace('preauth_code_files','error')}"
                            destination_prefix=f"{s3_input_files_prefix.replace('error','preauth_code_files')}"
                            s3_client = boto3.client("s3")
                            response=preauth_lib.move_s3_file(destination_prefix, s3_bucket, s3_input_files_prefix, s3_client)
                            logger.info(f"File Moved from to Error Folder=>{response}")
                            s3_input_files_prefix=f"{destination_prefix}"
                            input_file_name=f"{validation_stg_status[0]['file_name']}"
                            is_execution=True
                            is_reprocess_exec=True
            else:
                raise Exception('Invalid Load Category for Reprocess!')
    if reprocess_flag.strip().upper()=='Y' and int(failed_batch_id) > 0 :
        batch_id=failed_batch_id

    if is_execution:
        AWSGlueDataCatalog_preauth_Master_Config = glueContext.create_dynamic_frame.from_catalog(database=datacatalog_schema_name, table_name=datacatalog_config_table_name, transformation_ctx="AWSGlueDataCatalog_preauth_Master_Config")
        AWSGlueDataCatalog_preauth_Master_Config.printSchema()
        AWSGlueDataCatalog_preauth_Master_Config.show()
        config_master_df = AWSGlueDataCatalog_preauth_Master_Config.toDF()
        config_rec = config_master_df.first()
        
        AWSGlueDataCatalog_preauth_Config = glueContext.create_dynamic_frame.from_catalog(database=config_rec["datacatalog_schema_name"], table_name=config_rec["datacatalog_table_name"], transformation_ctx="AWSGlueDataCatalog_preauth_Config")
        AWSGlueDataCatalog_preauth_Config.printSchema()
        AWSGlueDataCatalog_preauth_Config.show()
        preauth_config_df = AWSGlueDataCatalog_preauth_Config.toDF()
        preauth_config_df.show(10, truncate=False)
        recordCount = preauth_config_df.count()
        logger.info(f"Config Record Count => {recordCount}")

        if is_reprocess_exec:
            logger.info(f'Deleting the data from staging tables for failure!')
            preauth_config_del_itr=preauth_config_df.filter(preauth_config_df.table_load_type==load_category).orderBy('order_seq_no', ascending=False).rdd.toLocalIterator()
            for del_record in preauth_config_del_itr:
                del_sql=del_record['reprocess_del_sql']
                del_sql=del_sql.replace('<failed_batch_id>',str(failed_batch_id))
                logger.info(f"SQL for deletion=>{del_sql}")
                preauth_lib.run_sql_query(del_sql, secrets)
                logger.info(f"data deleted for table=>{del_record['db_table_name']} for batch_id=>{failed_batch_id}")


        if load_category=='letters':
            preauth_config_df=preauth_config_df.filter(preauth_config_df.table_load_type=='letters')
            preauth_image_config_itr = preauth_config_df.orderBy(['order_seq_no']).rdd.toLocalIterator()
            logger.info(f"letters Category !")
            s3_client = boto3.client('s3')
            logger.info(f"path of folder to process!=>{s3_input_files_prefix}")
            response = s3_client.list_objects_v2(Bucket=s3_bucket,Prefix=s3_input_files_prefix)
            logger.info(f"response for searching index file=>{response}")
            objects = [object['Key'] for object in response['Contents'] if object['Key'].endswith('.txt')]
            if len(objects) > 1:
                logger.info(f"Found multiple index files to process!")
                response=preauth_lib.log_and_email(env_profile, 'Genex', load_category, reprocess_flag, batch_id, objects, step_function_info, step_function_execution_id, 'Glue Failed', 'Found multiple index files to process!', 'FATAL', sns_topic_arn, 'PreAuth Staging Load')
                raise Exception(" Found multiple index files to process!")
            elif len(objects) ==0:
                logger.info(f"No index files to process!")
                response=preauth_lib.log_and_email(env_profile, 'Genex', load_category, reprocess_flag, batch_id, None, step_function_info, step_function_execution_id, 'Glue Failed', 'No index files to process!', 'FATAL', sns_topic_arn, 'PreAuth Staging Load')
                raise Exception("No index files to process!")
            else:
                file_name=objects[0].split('/')[-1]
                if  file_name.startswith('TMI_IDX'):
                    logger.info(f"file for loading index file=>{file_name}")
                    s3_path=f"s3://{s3_bucket}/{s3_input_files_prefix}{file_name}"
                    logger.info(f"s3_path => {s3_path}")
                    init_df=spark.read.text(s3_path).toDF("pre_auth_data_str")
                    init_df.printSchema()
                    init_df.show(5,truncate=False)
                    for record in preauth_image_config_itr:
                        init_df.createOrReplaceTempView(record['df_table_name'])
                        preauth_df=spark.sql(record['transform_sql_select'])
                        if record['load_type']=='header':
                            preauth_df=preauth_df.withColumn("file_name", lit(input_file_name))
                        preauth_df.printSchema()
                        preauth_df.show(5,truncate=False)
                        record_count=preauth_df.count()
                        logger.info(f"record count=>{record_count}")
                        v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                        logger.info(f"v_load_timestamp => {v_load_timestamp}")
                        if is_reprocess_exec:
                            batch_id=failed_batch_id
                        preauth_df = preauth_df \
                        .withColumn("batch_id", lit(batch_id)) \
                        .withColumn("create_user", lit("Glue ETL-Pre Auth Staging load")) \
                        .withColumn("update_user", lit("Glue ETL-Pre Auth Staging load")) \
                        .withColumn("create_timestamp", lit(v_load_timestamp)) \
                        .withColumn("update_timestamp", lit(v_load_timestamp))
                        if record['load_type']=='detail':
                            logger.info(f"detail block!")
                            read_db_sql=record['db_read_sql']
                            logger.info(f"read_db_sql_detail => {read_db_sql}")
                            header_df = preauth_lib.get_df_db_data_with_query(spark,read_db_sql, secrets)
                            header_df.show(5,truncate=False)
                            final_df=header_df.join(preauth_df,preauth_df.lookup==header_df.claim_number,'inner')
                            preauth_df=final_df.drop(final_df.claim_number).drop(final_df.lookup).drop(final_df.batch_id)
                        preauth_df.printSchema()
                        preauth_df.show(5,truncate=False)
                        preauth_lib.load_data_to_rds(preauth_df, record["db_table_name"], secrets, mode)
                    status="COMPLETED"
                    destination_prefix=s3_input_files_prefix.replace('preauth_code_files','archive')
                    logger.info(f"destination Path for letters =>{destination_prefix}")
                    logger.info(f"Source Path for letters =>{s3_input_files_prefix}")
        else:
            logger.info(f"Notes Category !")
            trailer_record_count=0
            file_name=s3_input_files_prefix.split('/')[-1]
            logger.info(f"file for loading index file=>{file_name}")
            s3_path=f"s3://{s3_bucket}/{s3_input_files_prefix}"
            logger.info(f"s3_path => {s3_path}")
            init_df=spark.read.text(s3_path).toDF("pre_auth_data_str").withColumn("row_num", monotonically_increasing_id())
            #init_df.printSchema()
            #init_df.show(55,truncate=False)
            init_itr=init_df.orderBy("row_num").rdd.toLocalIterator()
            if is_reprocess_exec == False:
                for line in init_itr:
                    if(line['pre_auth_data_str'].strip().startswith('H')):
                        final_string.append(line['pre_auth_data_str'])
                    elif(line['pre_auth_data_str'].strip().startswith('I')):
                        row=str(line['pre_auth_data_str']).strip()
                        claim_number=row[1:15].strip()
                        unique_id=row[119:128].strip()
                        activity_name=row[129:160].strip()     
                        final_string.append(row)
                    elif(line['pre_auth_data_str'].strip().startswith('C')):
                        row=str(line['pre_auth_data_str']).strip()
                        record=f"C{claim_number}|{unique_id}|{activity_name}|{row}"    
                        final_string.append(record)
                    elif(line['pre_auth_data_str'].strip().startswith('T')):
                        row=str(line['pre_auth_data_str']).strip()
                        final_string.append(f"{row}\n")
                        trailer_record_count=str(row[2:7]).strip()
                        if trailer_record_count:
                            trailer_record_count=int(trailer_record_count)
                        logger.info(f"trailer_record_count=>{trailer_record_count}")
                joined_string = "\n".join(final_string)
                #logger.info(f"final data=>{joined_string}")
                s3_client = boto3.client("s3")
                response = s3_client.put_object(
                    Bucket=s3_bucket,
                    Key=s3_input_files_prefix,
                    Body=joined_string
                )
            #logger.info(f"response from s3 new file {response}")
            init_df=spark.read.text(s3_path).toDF("pre_auth_data_str").withColumn("row_num", monotonically_increasing_id())
            logger.info(f"Record for transformation!")
            #init_df.printSchema()
            init_df.show(10,truncate=False)
            preauth_config_df=preauth_config_df.filter(preauth_config_df.table_load_type=='notes')
            preauth_config_df.show(5,truncate=False)
            preauth_notes_config_itr = preauth_config_df.orderBy(['order_seq_no']).rdd.toLocalIterator()
            for record in preauth_notes_config_itr:
                init_df.createOrReplaceTempView(record['df_table_name'])
                preauth_df=spark.sql(record['transform_sql_select'])
                record_count=preauth_df.count()
                #logger.info(f"record count=>{record_count}")
                v_load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                #logger.info(f"v_load_timestamp => {v_load_timestamp}")
                if is_reprocess_exec:
                    batch_id=failed_batch_id
                preauth_df = preauth_df \
                .withColumn("batch_id", lit(batch_id)) \
                .withColumn("file_name", lit(input_file_name)) \
                .withColumn("create_user", lit("Glue ETL-Pre Auth Staging load")) \
                .withColumn("update_user", lit("Glue ETL-Pre Auth Staging load")) \
                .withColumn("create_timestamp", lit(v_load_timestamp)) \
                .withColumn("update_timestamp", lit(v_load_timestamp))
                if record['load_type']=='notes_header':
                    #logger.info(f"notes_header block!")
                    read_db_sql=record['db_read_sql']
                    notes_header_df = preauth_lib.get_df_db_data_with_query(spark,read_db_sql, secrets)
                    #notes_header_df.show(5,truncate=False)
                    notes_header_df_itr=notes_header_df.rdd.toLocalIterator()
                    for rec in notes_header_df_itr:
                        notes_file_header_id=rec["notes_file_header_id"]
                        logger.info(f"notes_file_header_id from file header=>{notes_file_header_id}")
                        preauth_df=preauth_df.withColumn("notes_file_header_id",lit(notes_file_header_id))
                        preauth_df=preauth_df.drop(preauth_df.batch_id).drop(preauth_df.file_name)
                if record['load_type']=='notes_detail':
                    read_db_sql=record['db_read_sql']
                    read_db_sql=read_db_sql.replace('<notes_file_header_id>',str(notes_file_header_id))
                    notes_header_id_df = preauth_lib.get_df_db_data_with_query(spark,read_db_sql, secrets)
                    #notes_header_id_df.show(5,truncate=False)
                    preauth_df=notes_header_id_df.join(preauth_df,(preauth_df.claim_number==notes_header_id_df.claim_number) & (preauth_df.unique_id==notes_header_id_df.unique_id) &(preauth_df.activity_name==notes_header_id_df.activity_name),'inner').select(notes_header_id_df.notes_header_id,preauth_df.notes,preauth_df.record_type,preauth_df.create_user,preauth_df.create_timestamp,preauth_df.update_user,preauth_df.update_timestamp)
                if record['load_type']=='notes_trailer':
                    preauth_df = spark.createDataFrame([["T"]],['record_type'])
                    preauth_df=preauth_df.withColumn("notes_file_header_id",lit(notes_file_header_id)).withColumn('record_count',lit(trailer_record_count))\
                    .withColumn("create_user", lit("Glue ETL-Pre Auth Staging load")) \
                    .withColumn("update_user", lit("Glue ETL-Pre Auth Staging load")) \
                    .withColumn("create_timestamp", lit(v_load_timestamp)) \
                    .withColumn("update_timestamp", lit(v_load_timestamp))
                preauth_df.show(5,truncate=False)
                preauth_lib.load_data_to_rds(preauth_df, record["db_table_name"], secrets, mode)  
        status="COMPLETED"
        batch_id=updt_batch_id
        if is_reprocess_exec:
            reprocess_status='COMPLETED_IN_REPROCESS'
            logger.info('Updating through inbuilt method!')
            preauth_lib.preauth_step_logging(secrets, failed_batch_id, failed_step_id, None, None, reprocess_status, 'Glue-ETL-Pre-Auth Staging Reprocess Load')

        preauth_lib.preauth_step_logging(secrets, batch_id, step_id, None, None, status, 'Glue-ETL-Pre-Auth Staging Load')
        logger.info(f"[SUCCESS] Successfully updated step_id {step_id} to status {status}.")
except Exception as e:
    batch_id=updt_batch_id
    status="FAILED"
    logger.error(f"Error committing the job:{e}")
    error_info = "Failed in PreAuth Staging Load"
    error_message = str(e)
    if load_category.lower() == 'notes':
        s3_client = boto3.client("s3")
        destination_prefix=s3_input_files_prefix.replace('preauth_code_files','error')
        logger.info(f"s3_input_files_prefix>{s3_input_files_prefix},destination_prefix=>{destination_prefix}")
        response=preauth_lib.move_s3_file(destination_prefix, s3_bucket, s3_input_files_prefix, s3_client)
        logger.info(f"File Moved from {s3_input_files_prefix} to {destination_prefix} Folder=>{response}")
    elif load_category.lower() == 'letters':
        destination_prefix=f"{s3_input_files_prefix.replace('preauth_code_files','error')}"
        s3_client = boto3.client("s3")
        for key in s3_client.list_objects(Bucket=s3_bucket,Prefix=s3_input_files_prefix)['Contents']:
            logger.info(f"Copied key=>{key['Key']}")
            file_name=key['Key'].split('/')[-1]
            archive_key = f"{destination_prefix}{file_name}"
            logger.info(f"archivekey==>{archive_key}")
            response=preauth_lib.archive_file(bucketName=s3_bucket,archivekey=archive_key,sourceKey=key['Key'])
            #logger.info(f"response=>{response}")
    preauth_lib.preauth_error_logging(secrets, batch_id, step_id, None, status, 'GLUE FAILED', error_info, error_message, 'Glue-ETL-Pre-Auth Staging Load', True)
    preauth_lib.preauth_step_logging(secrets, batch_id, step_id, None, None, status, 'Glue-ETL-Pre-Auth Staging Load')
    response = preauth_lib.log_and_email(env_profile, provider, load_category, reprocess_flag, batch_id, input_file_name, step_function_info, step_function_execution_id, error_info, error_message, "FATAL", sns_topic_arn, 'Glue-ETL-Pre-Auth Staging Load')
    logger.info(f"Email sent successfully!message_id:{response['MessageId']}")
    raise 
finally:
    logger.info("final  block")    
    if is_execution:
        if load_category=='notes' and status.upper()=="COMPLETED":
            s3_input_files_prefix=f"data/source/preauth/genex/preauth_code_files/notes/{input_file_name}"
            destination_prefix=f"data/source/preauth/genex/archive/notes/{input_file_name}"
            logger.info(f"s3_input_files_prefix>{s3_input_files_prefix},destination_prefix=>{destination_prefix}")
            response=preauth_lib.move_s3_file(destination_prefix, s3_bucket, s3_input_files_prefix, s3_client)
            logger.info(f"File Moved from to Error Folder=>{response}")
try:
    # Commit the job
    job.commit()
    logger.info("Job committed successfully")
except Exception as e:
    logger.error(f"Error committing the job:{e}")
    raise