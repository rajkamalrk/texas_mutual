CREATE TABLE txm_bitx_staging_logs.batch_staging_error_details (
    error_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT COMMENT 'Error ID',
    batch_id INT NOT NULL COMMENT 'Batch ID',
    error_code VARCHAR(100) COMMENT 'Error Code',
    error_type VARCHAR(100) COMMENT 'Error Type',
    error_message TEXT COMMENT 'Error Message',
    create_timestamp DATETIME(6) NOT NULL COMMENT 'Created On',
    update_timestamp DATETIME(6) NOT NULL COMMENT 'Updated On',
    create_user VARCHAR(50) NOT NULL COMMENT 'Created By',
    update_user VARCHAR(50) NOT NULL COMMENT 'Modified By',
    FOREIGN KEY (batch_id) REFERENCES batch_staging_details (batch_id),
    INDEX idx_batch_id (batch_id)
);