DROP TABLE IF EXISTS txm_bitx.invoice_status;

CREATE TABLE txm_bitx.invoice_status (
    status_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    txm_invoice_number BIGINT NOT NULL UNIQUE,
    status_type VARCHAR(100) NOT NULL,
    status VARCHAR(100) NOT NULL,
    create_user VARCHAR(50) NOT NULL,
    create_timestamp DATETIME(6) NOT NULL,
    update_user VARCHAR(50) NOT NULL,
    update_timestamp DATETIME(6) NOT NULL,
    FOREIGN KEY (txm_invoice_number) REFERENCES bill_header(txm_invoice_number),
    INDEX idx_txm_invoice_number (txm_invoice_number),
    INDEX idx_status_type (status_type,status)
);