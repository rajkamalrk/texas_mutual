CREATE TABLE txm_inbound_feeds_logs.batch_inbound_feeds_step_details (
  step_id int NOT NULL AUTO_INCREMENT,
  batch_id int NOT NULL,
  job_type varchar(50) NOT NULL,
  job_name varchar(255) NOT NULL,
  job_id varchar(255) NOT NULL,
  status varchar(50) NOT NULL,
  start_datetime datetime(6) NOT NULL,
  end_datetime datetime(6) DEFAULT NULL,
  create_user varchar(50) NOT NULL,
  create_timestamp datetime(6) NOT NULL,
  update_user varchar(50) NOT NULL,
  update_timestamp datetime(6) NOT NULL,
  PRIMARY KEY (step_id),
  KEY batch_id (batch_id),
  CONSTRAINT batch_inbound_feeds_step_details_ibfk_1 FOREIGN KEY (batch_id) REFERENCES txm_inbound_feeds_logs.batch_inbound_feeds_master (batch_id)
);