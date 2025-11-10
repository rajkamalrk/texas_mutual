DROP TABLE IF EXISTS txm_bitx.hospitalization_treatment_line;

CREATE TABLE txm_bitx.hospitalization_treatment_line (
    hospitalization_treatment_line_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    hospitalization_treatment_id INT,
    bill_id INT,
    line_number INT,
    procedure_code VARCHAR(8),
    hcpcs_modifier_code_1 VARCHAR(2),
    hcpcs_modifier_code_2 VARCHAR(2),
    hcpcs_modifier_code_3 VARCHAR(2),
    hcpcs_modifier_code_4 VARCHAR(4),
    service_from_date DATE,
    service_thru_date DATE,
    units_of_service DECIMAL(6,2),
    charge_for_service DECIMAL(10,2),
    revenue_code VARCHAR(4),
    description varchar(50),
    total_charge DECIMAL(11,2),
    create_user VARCHAR(50),
    create_timestamp DATETIME(6),
    update_user VARCHAR(50),
    update_timestamp DATETIME(6),
    FOREIGN KEY (hospitalization_treatment_id) REFERENCES hospitalization_treatment(hospitalization_treatment_id),
    INDEX idx_hospitalization_treatment_id (hospitalization_treatment_id),
    INDEX idx_bill_id (bill_id)
);