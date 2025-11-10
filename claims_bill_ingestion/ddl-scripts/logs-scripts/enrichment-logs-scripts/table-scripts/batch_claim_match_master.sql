CREATE TABLE txm_bitx_enrichment_logs.batch_claim_match_master (
  batch_id int AUTO_INCREMENT NOT NULL PRIMARY KEY,
  source varchar(25),
  trading_partner varchar(50) NOT NULL,
  file_header_id int,
  data_pipeline_id varchar(255),
  job_id varchar(255) NOT NULL,
  status varchar(50) NOT NULL,
  start_datetime datetime(6) NOT NULL,
  end_datetime datetime(6),
  update_timestamp datetime(6) NOT NULL,
  create_user varchar(50) NOT NULL,
  update_user varchar(50) NOT NULL,
  INDEX idx_master (source, trading_partner, file_header_id)
);

