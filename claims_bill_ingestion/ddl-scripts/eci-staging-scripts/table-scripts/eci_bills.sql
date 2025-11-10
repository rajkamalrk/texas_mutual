CREATE TABLE txm_bitx_eci_staging.eci_bills (
bill_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
header_id INT,
reported_claim_number VARCHAR(15),
txm_doc_type VARCHAR(25),
date_bill_received_by_payer DATE,
txm_invoice_number BIGINT,
create_user VARCHAR(50),
create_timestamp DATETIME(6),
update_user VARCHAR(50),
update_timestamp DATETIME(6),
FOREIGN KEY (header_id) REFERENCES eci_bills_header(header_id),
INDEX idx_header_id (header_id)
);