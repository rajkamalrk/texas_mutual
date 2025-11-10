DROP TABLE IF EXISTS txm_bitx_staging.jopari_line_level_physician_record_60;
CREATE TABLE txm_bitx_staging.jopari_line_level_physician_record_60 (
  line_level_physician_record_id Int NOT NULL PRIMARY KEY AUTO_INCREMENT,
  bill_header_id Int NOT NULL,
  record_type_code VARCHAR(2),
  unique_bill_id VARCHAR(15),
  line_number Int,
  physician_code VARCHAR(2),
  provider_first_name VARCHAR(80),
  provider_middle_name VARCHAR(25),
  provider_last_name VARCHAR(25),
  provider_degree VARCHAR(15),
  provider_state_license VARCHAR(30),
  provider_npi VARCHAR(10),
  provider_nabp VARCHAR(10),
  provider_dea_number VARCHAR(12),
  provider_taxonomy_code VARCHAR(10),
  create_user VARCHAR(50),
  update_user VARCHAR(50),
  create_timestamp DATETIME(6),
  update_timestamp DATETIME(6),
  FOREIGN KEY (bill_header_id) REFERENCES jopari_bill_header_record_10 (bill_header_id),
  INDEX idx_bill_header_id (bill_header_id)
);