DROP TABLE IF EXISTS txm_preauth_staging.preauth_notes_file_header;

CREATE TABLE txm_preauth_staging.preauth_notes_file_header (
	notes_file_header_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
	batch_id int,
	record_type	VARCHAR(1),
	transaction_code VARCHAR(1),
	file_name varchar(40),
	creation_date	TIMESTAMP,	
	date_range_from	TIMESTAMP,	
	date_range_thru	TIMESTAMP,	
	create_user	VARCHAR(50),
	create_timestamp	DATETIME(6),
	update_user	VARCHAR(50),
	update_timestamp	DATETIME(6)
);