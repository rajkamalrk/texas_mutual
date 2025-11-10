DROP TABLE IF EXISTS txm_preauth_logs.batch_preauth_step_details;

CREATE TABLE txm_preauth_logs.batch_preauth_step_details(
	step_id	INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
	batch_id INT  NOT NULL,
	job_type VARCHAR(50)  NOT NULL,
	job_name	VARCHAR(255)  NOT NULL,
	job_id	VARCHAR(255),
	status	VARCHAR(50)  NOT NULL,
	start_datetime	DATEtIME(6)  NOT NULL,
	end_datetime DATETIME(6),
	create_user	VARCHAR(50)  NOT NULL,
	create_timestamp DATETIME(6)  NOT NULL,
	update_user	VARCHAR(50)  NOT NULL,
	update_timestamp DATETIME(6)  NOT NULL,
    FOREIGN KEY (batch_id) REFERENCES txm_preauth_logs.batch_preauth_master(batch_id),
    INDEX idx_batch_id(batch_id)
    );