DROP TABLE IF EXISTS txm_ndc.ndc_summary_data;

CREATE TABLE txm_ndc.ndc_summary_data (
    ndc_summary_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    summary_create_date DATE,
    summary_expire_date DATE,
    summary_issue_date DATE,
    summary_kill_date DATE,
    product_file_mode VARCHAR(1),
    product_file_type VARCHAR(1),
    volume_number VARCHAR(5),
    supplement_number VARCHAR(2),
    m25data_record_count DECIMAL,
    create_timestamp DATETIME,
    create_user VARCHAR(50),
    update_timestamp DATETIME,
    update_user VARCHAR(50)
);
