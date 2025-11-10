DROP TABLE IF EXISTS txm_preauth_staging.preauth_notes_header;

CREATE TABLE txm_preauth_staging.preauth_notes_header(
	notes_header_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    notes_file_header_id INT ,
	record_type	VARCHAR(1),
	claim_number VARCHAR(20),
	reference	VARCHAR(10),
	claim_sequence_number VARCHAR(10),
	date_of_injury	DATE,	
	claimant_first_name	VARCHAR(10),
	claimant_last_name	VARCHAR(20),
	adjuster_last_name	VARCHAR(22),
	adjuster_first_name	VARCHAR(18),
	unique_id	VARCHAR(10),
	exposure_public_id varchar(50),
    activity_name	VARCHAR(50),
	activity_date	DATE,	
	claim_id	VARCHAR(15),
	create_user	VARCHAR(50),
	create_timestamp DATETIME(6),
	update_user	VARCHAR(50),
	update_timestamp DATETIME(6),
    FOREIGN KEY (notes_file_header_id) REFERENCES  txm_preauth_staging.preauth_notes_file_header (notes_file_header_id)
);