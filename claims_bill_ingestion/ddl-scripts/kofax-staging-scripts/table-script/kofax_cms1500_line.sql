DROP TABLE IF EXISTS txm_bitx_kofax_staging.kofax_cms1500_line;

CREATE TABLE txm_bitx_kofax_staging.kofax_cms1500_line(
    cms_line_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    cms_id INT,
    sequence_number INT,
    date_from DATE,
    date_to DATE,
    place_of_service_code VARCHAR(2),
    cptor_hcpcs VARCHAR(5),
    modifier_1 VARCHAR(2),
    modifier_2 VARCHAR(2),
    modifier_3 VARCHAR(2),
    modifier_4 VARCHAR(2),
    diagnosis_pointer_1 VARCHAR(1),
    diagnosis_pointer_2 VARCHAR(1),
    diagnosis_pointer_3 VARCHAR(1),
    diagnosis_pointer_4 VARCHAR(1),
    charges DECIMAL(9,2),
    days_or_units INT,
    national_provider_id INT,
    other_id VARCHAR(20),
    create_user VARCHAR(50),
    create_timestamp DATETIME(6),
    update_user VARCHAR(50),
    update_timestamp DATETIME(6),
    FOREIGN KEY (cms_id) REFERENCES kofax_cms1500(cms_id),
    INDEX idx_cms_id (cms_id)
);