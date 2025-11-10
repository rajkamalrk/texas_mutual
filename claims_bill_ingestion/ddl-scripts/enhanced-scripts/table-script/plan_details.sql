DROP TABLE IF EXISTS txm_bitx.plan_details;

CREATE TABLE txm_bitx.plan_details (
    plan_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    bill_id INT,
    release_of_info_certification VARCHAR(1),
    assignment_of_benefits_certification VARCHAR(1),
    prior_payments INT,
    estimated_amount_due INT,
    create_user VARCHAR(50),
    create_timestamp DATETIME(6),
    update_user VARCHAR(50),
    update_timestamp DATETIME(6),
    FOREIGN KEY (bill_id) REFERENCES bill_header(bill_id),
    INDEX idx_bill_id (bill_id)
);