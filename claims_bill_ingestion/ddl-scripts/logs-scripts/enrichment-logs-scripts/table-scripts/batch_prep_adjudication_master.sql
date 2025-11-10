CREATE TABLE txm_bitx_enrichment_logs.batch_prep_adjudication_master (
  batch_id int PRIMARY KEY AUTO_INCREMENT,
  source varchar(25),
  trading_partner varchar(50),
  file_header_id int,
  job_id varchar(255),
  status varchar(50),
  start_datetime datetime(6),
  end_datetime datetime(6),
  update_timestamp datetime(6),
  create_user varchar(50),
  update_user varchar(50),
  INDEX idx_master (source, trading_partner, file_header_id)
);