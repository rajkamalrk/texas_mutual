CREATE TABLE txm_bitx_kofax_staging.kofax_dwc066_attachments (
    attachment_id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
    dwc_id INT NOT NULL,
    document_item_type VARCHAR(20),
    serial_number VARCHAR(200) UNIQUE,
    create_user VARCHAR(50),
    update_user VARCHAR(50),
    create_timestamp DATETIME(6) NOT NULL,
    update_timestamp DATETIME(6) NOT NULL,
    FOREIGN KEY (dwc_id) REFERENCES txm_bitx_kofax_staging.kofax_dwc066(dwc_id),
    INDEX idx_dwc_id (dwc_id)
);