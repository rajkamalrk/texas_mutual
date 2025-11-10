CREATE TABLE txm_bitx_enrichment_logs.batch_image_creation_error_details (
  error_id int NOT NULL PRIMARY KEY AUTO_INCREMENT,
  batch_id int NOT NULL,
  bill_id int,
  txm_invoice_number bigint,
  source varchar(100),
  target varchar(100),
  image_type varchar(100),
  error_code varchar(100),
  error_type varchar(100),
  error_message TEXT,
  create_user varchar(50) NOT NULL,
  update_user varchar(50) NOT NULL,
  create_timestamp datetime(6) NOT NULL,
  update_timestamp datetime(6) NOT NULL,
  FOREIGN KEY (batch_id) REFERENCES batch_storage_request_master (batch_id),
  INDEX idx_batch_id (batch_id)
);