CREATE TABLE txm_bitx_enhanced_logs.batch_non_ebill_error_details (
  error_id int NOT NULL PRIMARY KEY AUTO_INCREMENT,
  batch_id int NOT NULL,
  bill_header_id int,
  error_code varchar(100),
  error_type varchar(100),
  error_message TEXT,
  create_user varchar(50) NOT NULL,
  update_user varchar(50) NOT NULL,
  create_timestamp datetime(6) NOT NULL,
  update_timestamp datetime(6) NOT NULL,
  FOREIGN KEY (batch_id) REFERENCES batch_non_ebill_master (batch_id),
  INDEX idx_batch_id (batch_id)
);