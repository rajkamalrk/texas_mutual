CREATE TABLE txm_ndc_logs.batch_ndc_step_details (
    step_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    batch_id INT NOT NULL,
    file_create_date DATE,
    job_type VARCHAR(50) NOT NULL,
    job_name VARCHAR(255) NOT NULL,
    job_id VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,
    start_datetime DATETIME(6) NOT NULL,
    end_datetime DATETIME(6),
    create_user VARCHAR(50) NOT NULL,
    create_timestamp DATETIME(6) NOT NULL,
    update_user VARCHAR(50) NOT NULL,
    update_timestamp DATETIME(6) NOT NULL,
    FOREIGN KEY (batch_id) REFERENCES txm_ndc_logs.batch_ndc_master(batch_id)
);
