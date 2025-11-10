DROP TABLE IF EXISTS txm_bitx.invoice_step_status;

CREATE TABLE txm_bitx.invoice_step_status (
    step_status_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    status_id INT NOT NULL,
    status_type VARCHAR(100) NOT NULL,
	step_info	VARCHAR(300) NOT NULL,
    step_status VARCHAR(100) NOT NULL,
    create_user VARCHAR(50) NOT NULL,
    create_timestamp DATETIME(6) NOT NULL,
    update_user VARCHAR(50) NOT NULL,
    update_timestamp DATETIME(6) NOT NULL,
    FOREIGN KEY (status_id) REFERENCES invoice_status(status_id),
    INDEX idx_status_id (status_id),
    INDEX idx_status_type (status_type)
);