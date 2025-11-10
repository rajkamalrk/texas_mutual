CREATE TABLE txm_bitx_enrichment_logs.batch_storage_request_error_details (
    error_id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
    batch_id INT NOT NULL,
    bill_id INT,
    txm_invoice_number BIGINT,
    billing_documents_id INT,
    source varchar(100),
	target varchar(100),
	storage_type varchar(100),
    error_code VARCHAR(100),
    error_type VARCHAR(100),
    error_message TEXT,
    create_user VARCHAR(50) NOT NULL,
    update_user VARCHAR(50) NOT NULL,
    create_timestamp DATETIME(6) NOT NULL,
    update_timestamp DATETIME(6) NOT NULL,
    FOREIGN KEY (batch_id) REFERENCES batch_storage_request_master(batch_id),
    INDEX idx_batch_id (batch_id)
);