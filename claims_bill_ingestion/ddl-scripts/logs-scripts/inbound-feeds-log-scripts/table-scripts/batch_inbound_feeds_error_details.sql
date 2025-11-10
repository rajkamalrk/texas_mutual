CREATE TABLE txm_inbound_feeds_logs.batch_inbound_feeds_error_details (
  error_id int NOT NULL AUTO_INCREMENT,
  batch_id int NOT NULL,
  step_id int DEFAULT NULL,
  txm_invoice_number bigint DEFAULT NULL,
  error_code varchar(100) DEFAULT NULL,
  error_info varchar(255) DEFAULT NULL,
  error_message text,
  create_user varchar(50) NOT NULL,
  create_timestamp datetime(6) NOT NULL,
  update_user varchar(50) NOT NULL,
  update_timestamp datetime(6) NOT NULL,
  PRIMARY KEY (error_id),
  KEY idx_batch_id (batch_id),
  KEY idx_step_id (step_id),
  CONSTRAINT batch_inbound_feeds_error_details_ibfk_1 FOREIGN KEY (batch_id) REFERENCES txm_inbound_feeds_logs.batch_inbound_feeds_master (batch_id),
  CONSTRAINT batch_inbound_feeds_error_details_ibfk_2 FOREIGN KEY (step_id) REFERENCES txm_inbound_feeds_logs.batch_inbound_feeds_step_details (step_id)
);