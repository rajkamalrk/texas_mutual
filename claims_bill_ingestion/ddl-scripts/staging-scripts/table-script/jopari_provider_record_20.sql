DROP TABLE IF EXISTS txm_bitx_staging.jopari_provider_record_20;
CREATE TABLE txm_bitx_staging.jopari_provider_record_20 (
	provider_record_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
	bill_header_id INT NOT NULL,
	record_type_code VARCHAR(2),	
	unique_bill_id VARCHAR(15),
	physician_code VARCHAR(2),
	first_name VARCHAR(80),
	middle_name VARCHAR(25),
	last_name VARCHAR(25),
	degree VARCHAR(15),
	state_license VARCHAR(30),
	npi VARCHAR(10),
	nabp VARCHAR(10),
	dea_number VARCHAR(12),
	taxonomy_code VARCHAR(10),
	create_user VARCHAR(50),
    update_user VARCHAR(50),
    create_timestamp DATETIME(6),
    update_timestamp DATETIME(6),
	FOREIGN KEY (bill_header_id) REFERENCES jopari_bill_header_record_10 (bill_header_id),
	INDEX idx_bill_header_id (bill_header_id)
);