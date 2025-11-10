
CREATE TABLE txm_bitx_enrichment_logs.batch_generate_invoice_master(
batch_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
source VARCHAR(25),
trading_partner VARCHAR(50) NOT NULL,
file_header_id INT,
job_id VARCHAR(255),
status VARCHAR(50) NOT NULL,
start_datetime datetime(6) NOT NULL,
end_datetime datetime(6),
update_timestamp datetime(6) NOT NULL,
create_user VARCHAR(50) NOT NULL,
update_user VARCHAR(50) NOT NULL,
INDEX idx_master (source, trading_partner, file_header_id)
);