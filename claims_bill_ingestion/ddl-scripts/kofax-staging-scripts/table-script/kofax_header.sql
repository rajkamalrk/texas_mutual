CREATE TABLE txm_bitx_kofax_staging.kofax_header (
    header_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    create_user VARCHAR(50),
    create_timestamp DATETIME(6),
    update_user VARCHAR(50),
    update_timestamp DATETIME(6)
);