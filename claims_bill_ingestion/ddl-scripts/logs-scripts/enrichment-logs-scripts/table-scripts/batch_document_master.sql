CREATE TABLE txm_bitx_enrichment_logs.batch_document_master (
    batch_id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
    source VARCHAR(25),
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
    INDEX idx_master (source, trading_partner, file_header_id)
);
