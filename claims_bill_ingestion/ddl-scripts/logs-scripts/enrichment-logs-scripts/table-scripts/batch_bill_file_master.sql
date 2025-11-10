CREATE TABLE txm_bitx_enrichment_logs.batch_bill_file_master (
  batch_id int NOT NULL PRIMARY KEY AUTO_INCREMENT COMMENT 'Batch ID',
  batch_number_file int NOT NULL DEFAULT '0',
  job_id varchar(255) NOT NULL COMMENT 'AWS Glue Job ID',
  status varchar(50) NOT NULL COMMENT 'Batch Status',
  start_datetime datetime(6) NOT NULL COMMENT 'Batch Start Timestamp',
  end_datetime datetime(6) COMMENT 'Batch End Timestamp',
  update_timestamp datetime(6) NOT NULL COMMENT 'Updated On',
  create_user varchar(50) NOT NULL COMMENT 'Created By',
  update_user varchar(50) NOT NULL COMMENT 'Modified By'
);