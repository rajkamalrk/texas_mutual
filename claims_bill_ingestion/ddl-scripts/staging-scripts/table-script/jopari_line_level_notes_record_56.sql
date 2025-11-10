DROP TABLE IF EXISTS txm_bitx_staging.jopari_line_level_notes_record_56;
CREATE TABLE txm_bitx_staging.jopari_line_level_notes_record_56 (
line_level_notes_record_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
bill_header_id INT NOT NULL,
record_type_code VARCHAR(2),
unique_bill_id VARCHAR(15),
line_num Int,
note_record_num Int,
note_type_code VARCHAR(3),
note_text VARCHAR(80),
create_user VARCHAR(50),
update_user VARCHAR(50),
create_timestamp DATETIME(6),
update_timestamp DATETIME(6),
FOREIGN KEY (bill_header_id) REFERENCES jopari_bill_header_record_10 (bill_header_id),
INDEX idx_bill_header_id (bill_header_id)
);