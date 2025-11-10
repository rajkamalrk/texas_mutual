DROP TABLE IF EXISTS txm_preauth_staging.preauth_notes_file_trailer;

CREATE TABLE txm_preauth_staging.preauth_notes_file_trailer(
	notes_file_trailer_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    notes_file_header_id INT,
	record_type	VARCHAR(1),
	record_count INT,	
	create_user	VARCHAR(50),
	create_timestamp DATETIME(6),
	update_user	VARCHAR(50),
	update_timestamp DATETIME(6),
    FOREIGN KEY (notes_file_header_id) REFERENCES txm_preauth_staging.preauth_notes_file_header(notes_file_header_id)
);