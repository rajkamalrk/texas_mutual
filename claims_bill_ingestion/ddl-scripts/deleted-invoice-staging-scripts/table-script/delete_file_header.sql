CREATE TABLE txm_deleted_invoice_staging.delete_file_header (
  header_id int NOT NULL AUTO_INCREMENT,
  batch_id int NOT NULL,
  record_type varchar(1) DEFAULT NULL,
  file_name varchar(30),
  creation_datetime datetime(6),
  create_user varchar(50) DEFAULT NULL,
  create_timestamp datetime(6) DEFAULT NULL,
  update_user varchar(50) DEFAULT NULL,
  update_timestamp datetime(6) DEFAULT NULL,
  PRIMARY KEY (header_id)
);