DROP TABLE IF EXISTS txm_preauth.preauth_letters_data;

CREATE TABLE txm_preauth.preauth_letters_data (
	id	INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
	header_id INT,	
	batch_id INT,	
	claim_number VARCHAR(40),
	claim_id VARCHAR(40),
    detail_id	INT,
	image_type	VARCHAR(3),
	receive_date	DATE,
	document_name	VARCHAR(70),
    file_receive_date	DATETIME ,
	number_of_pages	DECIMAL(5,0),
	partner_id	INT,
    txm_document_type	VARCHAR(40),
	document_type_description	VARCHAR(40),
    document_extension	VARCHAR(10),
	document_path	VARCHAR(200),
	document_serial_number	VARCHAR(200),
	nuxeo_status VARCHAR(15),
    create_user	VARCHAR(50),
	create_timestamp	DATETIME(6),
	update_user	VARCHAR(50),
	update_timestamp  DATETIME(6)
);