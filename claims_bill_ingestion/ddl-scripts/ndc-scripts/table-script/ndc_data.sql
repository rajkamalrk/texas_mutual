DROP TABLE IF EXISTS txm_ndc.ndc_data;

CREATE TABLE txm_ndc.ndc_data (
    ndc_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    ndc_code VARCHAR(11) NOT NULL UNIQUE,
    ndc_name VARCHAR(25),
    ndc_name_extension VARCHAR(25),
    ndc_item_status VARCHAR(1),
    ndc_status_change_date DATE,
    ndc_strength DECIMAL(13,5),
    ndc_unit_of_measure VARCHAR(11),
    ndc_record_status VARCHAR(1),
    create_timestamp DATETIME,
    create_user VARCHAR(50),
    update_timestamp DATETIME,
    update_user VARCHAR(50)
);
