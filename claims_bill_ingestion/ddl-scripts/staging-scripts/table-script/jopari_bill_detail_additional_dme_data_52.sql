DROP TABLE IF EXISTS txm_bitx_staging.jopari_bill_detail_additional_dme_data_52;
CREATE TABLE txm_bitx_staging.jopari_bill_detail_additional_dme_data_52 (
    bill_detail_additional_dme_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    bill_header_id INT NOT NULL,
    record_type_code VARCHAR(2),
    unique_bill_id VARCHAR(15),
    line_number Int,
    dme_code VARCHAR(15),
    length_of_med_Necessity Int,
    rental_boolean VARCHAR(1),
    rental_period_unit_of_measure VARCHAR(2),
    rental_period Int,
    dme_rental_price Decimal(10,2),
    dme_purchase_price Decimal(10,2),
    dme_rental_billing_freq VARCHAR(1),
    create_user VARCHAR(50),
    update_user VARCHAR(50),
    create_timestamp DATETIME(6),
    update_timestamp DATETIME(6),
    FOREIGN KEY (bill_header_id) REFERENCES jopari_bill_header_record_10 (bill_header_id),
    INDEX idx_bill_header_id (bill_header_id)
);