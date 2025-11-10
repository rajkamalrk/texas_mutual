CREATE TABLE txm_bitx_enrichment_logs.batch_document_error_detail (
    error_id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
    batch_id INT NOT NULL,
    bill_id INT,
    txm_invoice_number BIGINT,
    error_code VARCHAR(100),
    error_type VARCHAR(100),
    error_message TEXT,
    create_user VARCHAR(50) NOT NULL,
    update_user VARCHAR(50) NOT NULL,
    create_timestamp DATETIME(6) NOT NULL,
    update_timestamp DATETIME(6) NOT NULL,
    FOREIGN KEY (batch_id) REFERENCES batch_document_master(batch_id),
    INDEX idx_batch_id (batch_id)
);
