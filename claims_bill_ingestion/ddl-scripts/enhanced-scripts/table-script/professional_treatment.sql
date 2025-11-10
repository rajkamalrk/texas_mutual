DROP TABLE IF EXISTS txm_bitx.professional_treatment;

CREATE TABLE txm_bitx.professional_treatment (
    treatment_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    bill_id INT,
    from_service_date DATE,
    thru_service_date DATE,
    other_treatment_date DATE,
    date_unable_to_work_from DATE,
    date_unable_to_work_to DATE,
    outside_lab_charges DECIMAL(14,2),
    total_charges DECIMAL(14,2),
    amount_paid DECIMAL(14,2),
    external_cause_of_injury_2 VARCHAR(2),
    external_cause_of_injury_3 VARCHAR(2),
    external_cause_of_injury VARCHAR(2),
    create_user VARCHAR(50),
    create_timestamp DATETIME(6),
    update_user VARCHAR(50),
    update_timestamp DATETIME(6),
    FOREIGN KEY (bill_id) REFERENCES bill_header(bill_id),
    INDEX idx_bill_id (bill_id)
);
