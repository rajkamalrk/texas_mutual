CREATE TABLE txm_ndc_logs.batch_ndc_master (
    batch_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    provider VARCHAR(100) NOT NULL,
    file_create_date DATE,
    file_name VARCHAR(100),
    file_path VARCHAR(255),
    data_pipeline_id VARCHAR(255),
    status VARCHAR(50) NOT NULL,
    start_datetime DATETIME(6) NOT NULL,
    end_datetime DATETIME(6),
    create_user VARCHAR(50) NOT NULL,
    create_timestamp DATETIME(6) NOT NULL,
    update_user VARCHAR(50) NOT NULL,
    update_timestamp DATETIME(6)NOT NULL
);
