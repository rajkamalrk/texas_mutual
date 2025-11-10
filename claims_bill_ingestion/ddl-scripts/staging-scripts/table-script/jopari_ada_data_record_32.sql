DROP TABLE IF EXISTS txm_bitx_staging.jopari_ada_data_record_32;
CREATE TABLE txm_bitx_staging.jopari_ada_data_record_32 (
ada_data_record_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
bill_header_id INT NOT NULL,
record_type_code VARCHAR(2),
unique_bill_id VARCHAR(15),
ortho_total_month_treatment VARCHAR(15),
ortho_treatment_month_remaining VARCHAR(15),
services_in_claim VARCHAR(1),
tooth_status VARCHAR(50),
create_user VARCHAR(50),
update_user VARCHAR(50),
create_timestamp DATETIME(6),
update_timestamp DATETIME(6),
FOREIGN KEY (bill_header_id) REFERENCES jopari_bill_header_record_10 (bill_header_id),
INDEX idx_bill_header_id (bill_header_id)
);
