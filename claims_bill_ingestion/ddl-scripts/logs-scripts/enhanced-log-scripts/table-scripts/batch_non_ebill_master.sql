CREATE TABLE txm_bitx_enhanced_logs.batch_non_ebill_master (
  batch_id int NOT NULL PRIMARY KEY AUTO_INCREMENT,
  source varchar(50) NOT NULL,
  trading_partner varchar(50) ,
  file_header_id int ,
  data_pipeline_id varchar(255) ,
  job_id varchar(255) NOT NULL,
  status varchar(50) NOT NULL,
  start_datetime datetime(6) NOT NULL,
  end_datetime datetime(6) ,
  update_timestamp datetime(6) NOT NULL,
  create_user varchar(50) NOT NULL,
  update_user varchar(50) NOT NULL,
  INDEX idx_master (source, trading_partner, file_header_id)
);