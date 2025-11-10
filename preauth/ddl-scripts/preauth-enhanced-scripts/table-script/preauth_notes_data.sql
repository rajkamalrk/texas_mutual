DROP TABLE IF EXISTS txm_preauth.preauth_notes_data;

CREATE TABLE txm_preauth.preauth_notes_data (
	preauth_notes_data_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
	batch_id int,
	claim_number VARCHAR(15),
    source_type	VARCHAR(30),
	note_timestamp	TIMESTAMP,
	note_body	longtext,
    note_activity_source VARCHAR(15),
	process_by_cc_status	VARCHAR(1),
	map_type varchar(1),
	activity_pattern_code varchar(25),
	exposure_public_id varchar(50),
	claim_id varchar(50),
	note_subject varchar(70),
	note_related_to varchar(50),
	note_topic varchar(20),
	activity_related_to varchar(50),
	create_user	VARCHAR(50),
	create_timestamp	DATETIME(6),
	update_user	VARCHAR(50),
	update_timestamp	DATETIME(6)
);