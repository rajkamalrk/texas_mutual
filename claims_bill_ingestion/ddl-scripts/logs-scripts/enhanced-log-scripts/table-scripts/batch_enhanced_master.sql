CREATE TABLE txm_bitx_enhanced_logs.batch_enhanced_master (
    batch_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    trading_partner VARCHAR(50) NOT NULL,
    file_header_id INT,
    data_pipeline_id VARCHAR(255),
    job_id VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,
    start_datetime DATETIME(6) NOT NULL,
    end_datetime DATETIME(6),
    update_timestamp DATETIME(6) NOT NULL,
    create_user VARCHAR(50) NOT NULL,
    update_user VARCHAR(50) NOT NULL,
    INDEX idx_master (trading_partner, file_header_id)
);
