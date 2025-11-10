DROP TABLE IF EXISTS txm_bitx_staging.align_attachment;

CREATE TABLE txm_bitx_staging.align_attachment (
attachment_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
invoice_header_id INT NOT NULL,
record_type_code VARCHAR(1),
document_id VARCHAR(70),
document_type VARCHAR(10),
create_user VARCHAR(50),
create_timestamp DATETIME(6) NOT NULL,
update_user VARCHAR(50),
update_timestamp DATETIME(6) NOT NULL,
FOREIGN KEY (invoice_header_id) REFERENCES align_invoice_header(invoice_header_id),
INDEX idx_invoice_header_id (invoice_header_id)
);
