DROP TABLE IF EXISTS txm_bitx_staging.optum_file_header;

CREATE TABLE txm_bitx_staging.optum_file_header (
    file_header_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    file_name VARCHAR(100),
	file_creation_date datetime(3),
	create_user VARCHAR(50),
    update_user VARCHAR(50),
    create_timestamp DATETIME(6),
    update_timestamp DATETIME(6),
    INDEX idx_file_name (file_name)
);