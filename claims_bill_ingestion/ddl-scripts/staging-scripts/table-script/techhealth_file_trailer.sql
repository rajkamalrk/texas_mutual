DROP TABLE IF EXISTS txm_bitx_staging.techhealth_file_trailer;

CREATE TABLE txm_bitx_staging.techhealth_file_trailer (
header_trailer_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
file_header_id INT NOT NULL,
record_type_code varchar(1),
header_records int,
detail_records int,
document_records int,
create_user VARCHAR(50),
update_user VARCHAR(50),
create_timestamp DATETIME(6),
update_timestamp DATETIME(6),
FOREIGN KEY (file_header_id) REFERENCES techhealth_file_header(file_header_id),
INDEX idx_file_header_id (file_header_id)
);