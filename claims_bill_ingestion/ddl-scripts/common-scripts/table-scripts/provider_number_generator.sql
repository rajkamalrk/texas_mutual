CREATE TABLE txm_bitx_common.provider_number_generator (
     generated_provider_number bigint NOT NULL,
     created_at timestamp NULL DEFAULT CURRENT_TIMESTAMP,
     PRIMARY KEY (generated_provider_number)
   );