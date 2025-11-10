CREATE TABLE txm_bitx_provider_logs.batch_provider_error_details (
  error_id int NOT NULL PRIMARY KEY AUTO_INCREMENT COMMENT 'Error ID',
  batch_id int NOT NULL COMMENT 'Batch ID',
  error_code varchar(100) COMMENT 'Error Code',
  error_type varchar(100) COMMENT 'Error Type',
  error_message TEXT COMMENT 'Error Message',
  create_timestamp datetime(6) NOT NULL COMMENT 'Created On',
  update_timestamp datetime(6) NOT NULL COMMENT 'Updated On',
  create_user varchar(50) NOT NULL COMMENT 'Created By',
  update_user varchar(50) NOT NULL COMMENT 'Modified By',
  FOREIGN KEY (batch_id) REFERENCES batch_provider_details (batch_id),
  INDEX idx_batch_id (batch_id)
);