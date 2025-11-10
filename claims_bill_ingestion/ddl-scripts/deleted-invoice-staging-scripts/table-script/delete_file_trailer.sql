CREATE TABLE txm_deleted_invoice_staging.delete_file_trailer (
  trailer_id int NOT NULL AUTO_INCREMENT,
  header_id int DEFAULT NULL,
  record_type varchar(1) DEFAULT NULL,
  invoice_count int DEFAULT NULL,
  create_user varchar(50) DEFAULT NULL,
  create_timestamp datetime(6) DEFAULT NULL,
  update_user varchar(50) DEFAULT NULL,
  update_timestamp datetime(6) DEFAULT NULL,
  PRIMARY KEY (trailer_id),
  KEY header_id (header_id),
  CONSTRAINT delete_file_trailer_ibfk_1 FOREIGN KEY (header_id) REFERENCES txm_deleted_invoice_staging.delete_file_header (header_id)
);
