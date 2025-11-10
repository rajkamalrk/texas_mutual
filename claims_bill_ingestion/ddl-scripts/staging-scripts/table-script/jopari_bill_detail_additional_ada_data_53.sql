DROP TABLE IF EXISTS txm_bitx_staging.jopari_bill_detail_additional_ada_data_53;

CREATE TABLE txm_bitx_staging.jopari_bill_detail_additional_ada_data_53 (
    bill_detail_additional_ada_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    bill_header_id INT NOT NULL,
    record_type_code VARCHAR(2),
    unique_bill_id VARCHAR(15),
    line_num Int,
    oral_cavity_designation_code1 VARCHAR(3),
    oral_cavity_designation_code2 VARCHAR(3),
    oral_cavity_designation_code3 VARCHAR(3),
    oral_cavity_designation_code4 VARCHAR(3),
    oral_cavity_designation_code5 VARCHAR(3),
    prosthesis_crown_inlay_code VARCHAR(1),
    tooth_info VARCHAR(20),
    create_user VARCHAR(50),
    update_user VARCHAR(50),
    create_timestamp DATETIME(6),
    update_timestamp DATETIME(6),
    FOREIGN KEY (bill_header_id) REFERENCES jopari_bill_header_record_10 (bill_header_id),
    INDEX idx_bill_header_id (bill_header_id)
);
