CREATE TABLE txm_bitx_logs.batch_step_function_error_details (
    error_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT COMMENT 'Error ID',
    sf_batch_id INT NOT NULL,
    job_type VARCHAR(100) COMMENT 'Job Type - AWS Glue/Lambda',
    job_name VARCHAR(255) COMMENT 'AWS Glue Job Name/Lambda Execution Name',
    job_id VARCHAR(255) COMMENT 'AWS Glue Job ID/Lambda Execution ID',
    payload VARCHAR(1000) COMMENT 'Input Payload',
    error_code VARCHAR(100) COMMENT 'Error Code',
    error_info VARCHAR(255) COMMENT 'Error Info',
    error_message TEXT COMMENT 'Error Message',
    create_timestamp DATETIME(6) NOT NULL COMMENT 'Create Timestamp',
    update_timestamp DATETIME(6) NOT NULL COMMENT 'Update Timestamp',
    create_user VARCHAR(50) NOT NULL COMMENT 'Create User',
    update_user VARCHAR(50) NOT NULL COMMENT 'Update User',
    FOREIGN KEY (sf_batch_id) REFERENCES txm_bitx_logs.batch_step_function_master(sf_batch_id)
);