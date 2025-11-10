DROP TABLE IF EXISTS txm_bitx_staging.techhealth_attachment;

CREATE TABLE txm_bitx_staging.techhealth_attachment (
attachment_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
invoice_header_id INT NOT NULL,
record_type_code varchar(1),
document_id varchar(70),
document_type varchar(10),
create_user VARCHAR(50),
update_user VARCHAR(50),
create_timestamp DATETIME(6),
update_timestamp DATETIME(6),
FOREIGN KEY (invoice_header_id) REFERENCES techhealth_invoice_header(invoice_header_id),
INDEX idx_invoice_header_id (invoice_header_id)
);