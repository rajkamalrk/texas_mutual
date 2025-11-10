DROP TABLE IF EXISTS txm_bitx_staging.jopari_file_trailer_record_90;
CREATE TABLE txm_bitx_staging.jopari_file_trailer_record_90 (
file_Trailer_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
file_header_id INT NOT NULL,
record_type_code VARCHAR(2),
num_of_bills_in_batch Int,
num_of_bill_lines_in_batch Int,
dollar_amount_billed DECIMAL(14,2),
create_user VARCHAR(50),
update_user VARCHAR(50),
create_timestamp DATETIME(6),
update_timestamp DATETIME(6),
FOREIGN KEY (file_header_id) REFERENCES jopari_file_header_record_01 (file_header_id),
INDEX idx_file_header_id (file_header_id)
);