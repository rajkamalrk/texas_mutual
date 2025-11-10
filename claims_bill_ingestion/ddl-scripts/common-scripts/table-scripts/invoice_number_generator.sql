CREATE TABLE txm_bitx_common.invoice_number_generator (
  invoice_number bigint NOT NULL AUTO_INCREMENT,
  created_at timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (invoice_number)
);

ALTER TABLE txm_bitx_common.invoice_number_generator AUTO_INCREMENT = 900000000000;