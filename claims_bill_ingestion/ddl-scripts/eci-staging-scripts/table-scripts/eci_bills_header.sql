CREATE TABLE txm_bitx_eci_staging.eci_bills_header (
header_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
create_user VARCHAR(50),
create_timestamp DATETIME(6),
update_user VARCHAR(50),
update_timestamp DATETIME(6)
);