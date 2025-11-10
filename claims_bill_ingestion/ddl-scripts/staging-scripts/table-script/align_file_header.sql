DROP TABLE IF EXISTS txm_bitx_staging.align_file_header;

CREATE TABLE txm_bitx_staging.align_file_header(
file_header_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
record_type_code VARCHAR(1),
system_indicator VARCHAR(1),
file_creation_date DATE,
file_name VARCHAR(100),
create_user VARCHAR(50),
create_timestamp DATETIME(6) NOT NULL,
update_user VARCHAR(50),
update_timestamp DATETIME(6) NOT NULL,
INDEX idx_file_creation_date (file_creation_date)
);
