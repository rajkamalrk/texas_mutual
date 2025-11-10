DROP TABLE IF EXISTS txm_bitx_staging.techhealth_file_header;

CREATE TABLE txm_bitx_staging.techhealth_file_header (
file_header_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
record_type_code varchar(1),
system_indicator varchar(1),
file_creation_date date,
file_name VARCHAR(100),
create_user VARCHAR(50),
update_user VARCHAR(50),
create_timestamp DATETIME(6),
update_timestamp DATETIME(6),
INDEX idx_file_creation_date (file_creation_date)
);


