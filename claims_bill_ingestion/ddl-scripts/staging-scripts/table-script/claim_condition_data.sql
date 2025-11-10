create table txm_bitx_staging.claim_condition_data(
claim_condition_data_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
claim_data_id INT,
conditional_effective_date DATE,
conditional_expiry_date DATE,
conditional_type_code VARCHAR(100),
conditional_type_description VARCHAR(60),
create_user VARCHAR(50),
create_timestamp datetime(6),
update_user VARCHAR(50),
update_timestamp datetime(6),
FOREIGN KEY (claim_data_id) REFERENCES txm_bitx_staging.claim_data(claim_data_id),
INDEX idx_claim_data_id (claim_data_id)
);