CREATE TABLE txm_bitx_enhanced_logs.batch_enhanced_error_details (
    error_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT ,
    batch_id INT NOT NULL,
    bill_header_id INT,
    error_code VARCHAR(100),
    error_type VARCHAR(100),
    error_message TEXT,
    create_user VARCHAR(50) NOT NULL,
    update_user VARCHAR(50) NOT NULL,
    create_timestamp DATETIME(6) NOT NULL,
    update_timestamp DATETIME(6) NOT NULL,
    FOREIGN KEY (batch_id) REFERENCES batch_enhanced_master(batch_id),
    INDEX idx_batch_id (batch_id)
);
