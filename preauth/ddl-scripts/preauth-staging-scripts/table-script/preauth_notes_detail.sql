DROP TABLE IF EXISTS txm_preauth_staging.preauth_notes_detail;

CREATE TABLE txm_preauth_staging.preauth_notes_detail (
	notes_detail_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    notes_header_id INT,
	record_type	VARCHAR(1),
	notes	VARCHAR(1699),
	create_user	VARCHAR(50),
	create_timestamp DATETIME(6),
	update_user	VARCHAR(50),
	update_timestamp DATETIME(6),
    FOREIGN KEY (notes_header_id) REFERENCES txm_preauth_staging.preauth_notes_header(notes_header_id)
);