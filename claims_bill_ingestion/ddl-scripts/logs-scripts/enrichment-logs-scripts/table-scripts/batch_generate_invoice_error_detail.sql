CREATE TABLE txm_bitx_enrichment_logs.batch_generate_invoice_error_details(
error_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
batch_id INT NOT NULL,
error_info VARCHAR(100),
error_message TEXT,
create_user VARCHAR(50) NOT NULL,
update_user VARCHAR(50) NOT NULL,
create_timestamp datetime(6) NOT NULL,
update_timestamp datetime(6) NOT NULL,
FOREIGN KEY (batch_id) REFERENCES batch_generate_invoice_master(batch_id),
INDEX idx_batch_id (batch_id)
);