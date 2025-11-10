DROP TABLE IF EXISTS kofax_dwc066_line;

CREATE TABLE txm_bitx_kofax_staging.kofax_dwc066_line(
    dwc_line_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    dwc_id INT,
    sequence_number INT,
    dispensed_aswritten_code VARCHAR(1),
    date_filled DATE,
    generic_ndc VARCHAR(15),
    name_brand_ndc VARCHAR(15),
    quantity INT,
    days_supply INT,
    drugname_and_strength VARCHAR(30),
    prescription_number VARCHAR(10),
    amount_billed DECIMAL(11,2),
    create_user VARCHAR(50),
    create_timestamp DATETIME(6),
    update_user VARCHAR(50),
    update_timestamp DATETIME(6),
    FOREIGN KEY (dwc_id) REFERENCES kofax_dwc066(dwc_id),
    INDEX idx_dwc_id (dwc_id)
);