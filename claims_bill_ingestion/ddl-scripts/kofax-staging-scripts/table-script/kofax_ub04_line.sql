DROP TABLE IF EXISTS kofax_ub04_line;

CREATE TABLE txm_bitx_kofax_staging.kofax_ub04_line(
    ub_line_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    ub_id INT,
    sequence_number INT,
    revenue_code VARCHAR(4),
    description VARCHAR(50),
    hcpcs_code VARCHAR(5),
    service_date DATE,
    units_service INT,
    total_charge DECIMAL(11,2),
    create_user VARCHAR(50),
    create_timestamp DATETIME(6),
    update_user VARCHAR(50),
    update_timestamp DATETIME(6),
    FOREIGN KEY (ub_id) REFERENCES kofax_ub04(ub_id),
    INDEX idx_ub_id (ub_id)
);