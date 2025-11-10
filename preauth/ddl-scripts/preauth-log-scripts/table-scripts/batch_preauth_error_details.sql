DROP TABLE IF EXISTS txm_preauth_logs.batch_preauth_error_details;

CREATE TABLE txm_preauth_logs.batch_preauth_error_details(
	error_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
	batch_id INT NOT NULL,
	step_id	INT,
	claim_number VARCHAR(30),
	error_code	VARCHAR(100),
	error_info	VARCHAR(255),
	error_message	TEXT,
	create_user	VARCHAR(50) NOT NULL,
	create_timestamp DATETIME(6) NOT NULL,
	update_user	VARCHAR(50) NOT NULL,
	update_timestamp DATETIME(6) NOT NULL,
    FOREIGN KEY (batch_id) REFERENCES txm_preauth_logs.batch_preauth_master (batch_id),
    FOREIGN KEY (step_id) REFERENCES txm_preauth_logs.batch_preauth_step_details (step_id),
    INDEX idx_batch_id(batch_id),
    INDEX idx_step_id(step_id)
);