CREATE TABLE txm_bitx_staging_logs.batch_staging_details (
    batch_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT COMMENT 'Batch ID',
    trading_partner VARCHAR(50) NOT NULL COMMENT 'Trading Partner',
    file_header_id INT COMMENT 'File Header ID (TP)',
    file_name VARCHAR(100) NOT NULL COMMENT 'File Name',
    file_path VARCHAR(255) NOT NULL COMMENT 'File Path',
    data_pipeline_id VARCHAR(255) COMMENT 'AWS Step Functions ID',
    job_id VARCHAR(255) NOT NULL COMMENT 'AWS Glue Job ID',
    status VARCHAR(50) NOT NULL COMMENT 'Batch Status',
    start_datetime DATETIME(6) NOT NULL COMMENT 'Batch Start Timestamp',
    end_datetime DATETIME(6) COMMENT 'Batch End Timestamp',
    update_timestamp DATETIME(6) NOT NULL COMMENT 'Updated On',
    create_user VARCHAR(50) NOT NULL COMMENT 'Created By',
    update_user VARCHAR(50) NOT NULL COMMENT 'Modified By',
    INDEX idx_trading_partner (trading_partner),
    INDEX idx_file_header_id (file_header_id),
    INDEX idx_master (trading_partner, file_name)
);