DROP TABLE IF EXISTS txm_bitx_staging.align_file_trailer;

CREATE TABLE txm_bitx_staging.align_file_trailer (
file_trailer_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
file_header_id INT NOT NULL,
record_type_code VARCHAR(1),
header_records INT,
detail_records INT,
document_records INT,
create_user VARCHAR(50),
update_user VARCHAR(50),
create_timestamp DATETIME(6),
update_timestamp DATETIME(6),
FOREIGN KEY (file_header_id) REFERENCES align_file_header (file_header_id),
INDEX idx_file_header_id (file_header_id)
);