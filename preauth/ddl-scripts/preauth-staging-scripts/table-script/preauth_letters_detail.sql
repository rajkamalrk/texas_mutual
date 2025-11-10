DROP TABLE IF EXISTS txm_preauth_staging.preauth_letters_detail;

CREATE TABLE txm_preauth_staging.preauth_letters_detail (
	detail_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,	
	header_id INT,	
	record_type	VARCHAR(1),
	image_type	VARCHAR(3),
	document_type VARCHAR(10),
	datetime_received DATETIME,	
	image_last_upload_datetime	DATETIME,
	description	VARCHAR(40),
	image_name	VARCHAR(70),
	create_user	VARCHAR(50),
	create_timestamp DATETIME(6),
	update_user	VARCHAR(50),
	update_timestamp DATETIME(6),
	FOREIGN KEY (header_id) REFERENCES txm_preauth_staging.preauth_letters_header(header_id)
);