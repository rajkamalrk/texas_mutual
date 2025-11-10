DROP TABLE IF EXISTS txm_bitx_staging.jopari_file_header_record_01;
CREATE TABLE txm_bitx_staging.jopari_file_header_record_01 (
file_header_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
record_type_code VARCHAR(2),
schema_version VARCHAR(5),
file_creation_dt_time datetime(3),
file_batch_id VARCHAR(32),
file_name VARCHAR(100),
claim_administrator_unique_id VARCHAR(32),
claim_administrator_ofc_unique_id VARCHAR(32),
create_user VARCHAR(50),
update_user VARCHAR(50),
create_timestamp DATETIME(6),
update_timestamp DATETIME(6),
INDEX idx_file_batch_id (file_batch_id)
);
