CREATE TABLE txm_inbound_feeds_logs.batch_inbound_feeds_master (
  batch_id int NOT NULL AUTO_INCREMENT,
  failed_batch_id int,
  reprocess_flag VARCHAR(1),
  feed_type VARCHAR(50) DEFAULT NULL,
  provider varchar(100) NOT NULL,
  file_name varchar(100) DEFAULT NULL,
  file_path varchar(255) DEFAULT NULL,
  data_pipeline_id varchar(255) DEFAULT NULL,
  status varchar(50) NOT NULL,
  start_datetime datetime(6) NOT NULL,
  end_datetime datetime(6) DEFAULT NULL,
  create_user varchar(50) NOT NULL,
  create_timestamp datetime(6) NOT NULL,
  update_user varchar(50) NOT NULL,
  update_timestamp datetime(6) NOT NULL,
  PRIMARY KEY (batch_id)
);