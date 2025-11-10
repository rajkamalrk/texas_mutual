CREATE TABLE txm_ndc_logs.batch_ndc_error_details (
    error_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    batch_id INT NOT NULL,
    step_id INT NOT NULL,
    error_code VARCHAR(100),
    error_info VARCHAR(255),
    error_message TEXT,
    create_user VARCHAR(50) NOT NULL,
    create_timestamp DATETIME(6) NOT NULL,
    update_user VARCHAR(50) NOT NULL,
    update_timestamp DATETIME(6) NOT NULL,
    FOREIGN KEY (batch_id) REFERENCES txm_ndc_logs.batch_ndc_master(batch_id),
    FOREIGN KEY (step_id) REFERENCES txm_ndc_logs.batch_ndc_step_details(step_id)
);