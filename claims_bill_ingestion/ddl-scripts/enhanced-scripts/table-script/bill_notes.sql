CREATE TABLE IF NOT EXISTS txm_bitx.bill_notes (
    note_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    bill_id INT,
    note_record_number INT,
    note_text VARCHAR(100),
    note_type_code VARCHAR(2),
    create_user VARCHAR(50),
    create_timestamp DATETIME(6),
    update_user VARCHAR(50),
    update_timestamp DATETIME(6),
    FOREIGN KEY (bill_id) REFERENCES bill_header(bill_id),
    INDEX idx_bill_id (bill_id)
    );