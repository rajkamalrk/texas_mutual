CREATE TABLE txm_bitx_enrichment_logs.batch_provider_request_error_details (
  error_id int NOT NULL PRIMARY KEY AUTO_INCREMENT,
  batch_id int,
  bill_id int,
  txm_invoice_number bigint,
  target varchar(100),
  source varchar(100),
  storage_type varchar(100),
  error_code varchar(100),
  error_type varchar(100),
  error_message TEXT,
  create_user varchar(50),
  update_user varchar(50),
  create_timestamp datetime(6),
  update_timestamp datetime(6),
  FOREIGN KEY (batch_id) REFERENCES batch_provider_request_master (batch_id),
  INDEX idx_batch_id (batch_id)
);