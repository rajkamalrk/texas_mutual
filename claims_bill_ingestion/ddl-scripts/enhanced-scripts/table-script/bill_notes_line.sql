CREATE TABLE IF NOT EXISTS txm_bitx.bill_notes_line (
    line_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    bill_id INT,
    line_number INT,
    line_text VARCHAR(100),
    line_type_code VARCHAR(3),
    line_record_number DECIMAL(2,0),
    create_user VARCHAR(50),
    create_timestamp DATETIME(6),
    update_user VARCHAR(50),
    update_timestamp DATETIME(6),
    FOREIGN KEY (bill_id) REFERENCES bill_header(bill_id),
    INDEX idx_bill_id (bill_id)
);
