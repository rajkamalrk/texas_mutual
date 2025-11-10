DROP TABLE IF EXISTS txm_bitx.billing_documents;

CREATE TABLE txm_bitx.billing_documents (
    billing_documents_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    bill_id INT,
    file_type VARCHAR(10) DEFAULT 'ATTACHMENT',
    attachment_record_number INT,
    document_type_code VARCHAR(10),
    document_transmission_code VARCHAR(2),
    attachment_control_number VARCHAR(80),
    attachment_description VARCHAR(80),
    document_path varchar(300),
    document_serial_number VARCHAR (200),
    txm_doc_type varchar(25),
    bill_xml_document_path  varchar(300),
    image_creation_response VARCHAR(200),
    create_user VARCHAR(50),
    create_timestamp DATETIME(6),
    update_user VARCHAR(50),
    update_timestamp DATETIME(6),
    FOREIGN KEY (bill_id) REFERENCES bill_header(bill_id),
    INDEX idx_bill_id (bill_id)
);
