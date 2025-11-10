CREATE TABLE txm_deleted_invoice_staging.delete_invoice_detail (
  detail_id int NOT NULL AUTO_INCREMENT,
  header_id int DEFAULT NULL,
  record_type varchar(1) DEFAULT NULL,
  invoice_number bigint DEFAULT NULL,
  create_user varchar(50) DEFAULT NULL,
  create_timestamp datetime(6) DEFAULT NULL,
  update_user varchar(50) DEFAULT NULL,
  update_timestamp datetime(6) DEFAULT NULL,
  PRIMARY KEY (detail_id),
  KEY header_id (header_id),
  CONSTRAINT delete_invoice_detail_ibfk_1 FOREIGN KEY (header_id) REFERENCES txm_deleted_invoice_staging.delete_file_header (header_id)
);
