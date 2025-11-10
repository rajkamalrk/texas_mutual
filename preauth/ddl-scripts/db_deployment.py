from configparser import ConfigParser
import mysql.connector as mysql
import os
import logging
import sys
from datetime import datetime
configName = sys.argv[1]   #getting configuration file like bill_config.ini or invoice_config.ini
configName2 = sys.argv[2]  # getting environment credential configuration file
dbEnvConfig = sys.argv[3]  # getting environment name to connect DB credentials
# logger configuration
current_datetime = datetime.now().strftime("%Y-%m-%d %H-%M-%S")
str_current_datetime = str(current_datetime)
LOG_FORMAT = "%(levelname)s %(asctime)s - %(message)s"
logging.basicConfig(filename='LogFiles/'+'ScriptExecution_'+str_current_datetime+'.log',level = logging.INFO, format = LOG_FORMAT,filemode = 'w')
logger=logging.getLogger()
logger.info('--------------Script is Started----------------')
# reading configuration file to get DB credentials and files folder 
config = ConfigParser()
config.read(configName)
config.read(configName2)
# DB connection
dbConnect=mysql.connect(user=config.get(dbEnvConfig, 'user'),password=config.get(dbEnvConfig, 'password',raw = True),host=config.get(dbEnvConfig, 'host'))
# schema folder files path
if config.has_section("sqlschemapath"):
  schemaLocation=os.listdir(config.get('sqlschemapath','path'))
else:
  schemaLocation = []
# ddl folder files path
DDLLocation=os.listdir(config.get('sqlddlpath','path'))
#variable declaration
successList =[]
failedDict ={}
failedFiles = 0
# getting cursor object to execute sql queries
cursor=dbConnect.cursor(buffered=True) 
try :
    logger.info('STARTED THE SCHEMA FOLDER FILES PROCESSING')     
    for filename in schemaLocation:
      if filename.endswith('.sql'):  
        logger.info('%s : file started execution in DB', filename)   
        if os.path.getsize(config.get('sqlschemapath','path')+filename) == 0:
          logger.info('%s : is empty, can not process into DB', filename)     
        else :
          fd = open(config.get('sqlschemapath','path')+filename, 'r')
          sqlFile = fd.read()
          sqlCommands = sqlFile.split(';')
          for command in sqlCommands:
            if command.strip() != '':  
              cursor.execute(command)  
        logger.info('%s : file completed execution in DB', filename)                                
      else :
        logger.info('%s : is not sql extention file', filename)                   
except Exception as e:
    print("Schema Folder files went something wrong",e)
    sys.exit(0)
logger.info('Completed THE SCHEMA FOLDER FILES PROCESSING')
with open(config.get('tablenames','path')) as f:
    tableNames = [line.rstrip('\n')  for line in f]
tableNamesList = list(filter(None, tableNames))    
if schemaLocation:
  logger.info('STARTED THE DDL FOLDER FILEs PROCESSING')
  logger.info('Total Count of Files is %s :',len(tableNamesList))    
  if len(DDLLocation)>=len(tableNamesList):
    for filename in tableNamesList:
      if filename in DDLLocation: 
        if filename.endswith('.sql'):
          if os.path.getsize(config.get('sqlddlpath','path')+filename) == 0:
            logger.info('%s : is empty, can not process into DB', filename)
            failedDict[filename]='File is empty, not process INTO DB'
          else :
            fd = open(config.get('sqlddlpath','path')+filename, 'r')
            sqlFile = fd.read()
            sqlCommands = sqlFile.split(';')
            logger.info('%s : file started execution in DB', filename)
            for command in sqlCommands:
              if command.strip() != '': 
                cursor=dbConnect.cursor()             
                try:
                  cursor.execute(command)
                except :
                  failedDict[filename]='File is not executed in DB, because of syntax or content not correct'
                  failedFiles = failedFiles +1
                  
            logger.info('%s : file completed execution in DB', filename)
                
            successList.append(filename)    
        else :  
          logger.info('%s : is not sql extention file', filename)
          failedDict[filename]='File is not sql extention'
      else :
        logger.info('%s : file is not available', filename)   
        failedDict[filename]='File is not available in DDLScriptFolder'  
  else :
    logger.info('All Table are not Available in SQLDDLScript Folder')
    raise ValueError('Failed Execution due to the unmatched count of tables')
  logger.info('Completed THE DDL FOLDER FILEs PROCESSING')
else:
  logger.info('STARTED THE Stored Procedures FILEs PROCESSING')
  logger.info('Total Count of Files is %s :',len(tableNamesList))    
  if len(DDLLocation)>=len(tableNamesList):
    for filename in tableNamesList:
      if filename in DDLLocation: 
        if filename.endswith('.sql'):
          if os.path.getsize(config.get('sqlddlpath','path')+filename) == 0:
            logger.info('%s : is empty, can not process into DB', filename)
            failedDict[filename]='File is empty, not process INTO DB'
          else :
            fd = open(config.get('sqlddlpath','path')+filename, 'r')
            sqlCommands = []
            file_sql = fd.read()
            sqlCommands.append(file_sql.split('\n',2)[0])
            sqlCommands.append(file_sql.split('\n',2)[-1].replace('DELIMITER $$', '').replace('END $$', 'END').replace('END$$', 'END').replace('DELIMITER ;',''))
            logger.info('%s : file started execution in DB', filename)
            cursor=dbConnect.cursor()
            for command in sqlCommands:
              if command.strip() != '': 
                cursor=dbConnect.cursor()             
                try:
                  cursor.execute(command)
                except Exception as e:
                  logger.info(e)
                  failedDict[filename]='File is not executed in DB, because of syntax or content not correct'
                  failedFiles = failedFiles +1
                  
            logger.info('%s : file completed execution in DB', filename)
                
            successList.append(filename)    
        else :  
          logger.info('%s : is not sql extention file', filename)
          failedDict[filename]='File is not sql extention'
      else :
        logger.info('%s : file is not available', filename)   
        failedDict[filename]='File is not available in DDLScriptFolder'  
  else :
    logger.info('All Stored Procs are not Available in SQLDDLScript Folder')
    raise ValueError('Failed Execution due to the unmatched count of Stored Procs')
  logger.info('Completed THE Stored Procedures FILEs PROCESSING')
logger.info('-------------completed Execution of Script---------------------')      
logger.info('Successfully Inserted Files INTO DB Count is : %s',len(successList)-failedFiles) 
logger.info(successList)
logger.info('Failed to Insert Files INTO DB Count is : %s',len(failedDict))
logger.info(failedDict)
if(len(successList)-failedFiles==len(tableNamesList) or dbConnect==True):
  print('Successfully All Files are Inserted INTO DB')
  cursor.close()
  dbConnect.close()
else :
  print('Failed TO Inserted Files INTO DB')
  cursor.close()
  dbConnect.close()
  