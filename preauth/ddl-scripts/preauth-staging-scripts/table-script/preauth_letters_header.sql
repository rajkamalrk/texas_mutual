DROP TABLE IF EXISTS txm_preauth_staging.preauth_letters_header;

CREATE TABLE txm_preauth_staging.preauth_letters_header (
	header_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
	batch_id INT,
	record_type	VARCHAR(1),
	claim_number VARCHAR(30),
	date_of_injury	DATE,
	file_name VARCHAR(30),
	create_user	VARCHAR(50),
	create_timestamp DATETIME,
	update_user	VARCHAR(50),
	update_timestamp DATETIME
);