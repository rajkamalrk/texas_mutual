CREATE TABLE txm_bitx_enrichment_logs.batch_prep_adjudication_error_details (
  error_id int NOT NULL PRIMARY KEY AUTO_INCREMENT,
  batch_id int NOT NULL,
  error_info varchar(1000),
  error_message TEXT,
  create_user varchar(50) NOT NULL,
  update_user varchar(50) NOT NULL,
  create_timestamp datetime(6) NOT NULL,
  update_timestamp datetime(6) NOT NULL,
  FOREIGN KEY (batch_id) REFERENCES batch_prep_adjudication_master (batch_id),
  INDEX idx_batch_id (batch_id)
);