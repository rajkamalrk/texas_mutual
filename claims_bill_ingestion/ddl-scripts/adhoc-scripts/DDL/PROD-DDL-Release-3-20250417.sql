-- renaming cols
ALTER TABLE txm_bitx_kofax_staging.kofax_ub04
RENAME COLUMN procedures_other_code  TO procedures_other_code_A; 

ALTER TABLE txm_bitx_kofax_staging.kofax_ub04
RENAME COLUMN procedures_other_date  TO procedures_other_date_A;
 
-- adding new cols
ALTER TABLE txm_bitx_kofax_staging.kofax_ub04
ADD COLUMN procedures_other_code_B VARCHAR(7) AFTER procedures_other_date_A;

ALTER TABLE txm_bitx_kofax_staging.kofax_ub04
ADD COLUMN procedures_other_date_B DATE AFTER procedures_other_code_B; 

ALTER TABLE txm_bitx_kofax_staging.kofax_ub04
ADD COLUMN procedures_other_code_C VARCHAR(7) AFTER procedures_other_date_B; 

ALTER TABLE txm_bitx_kofax_staging.kofax_ub04
ADD COLUMN procedures_other_date_C DATE AFTER procedures_other_code_C; 

ALTER TABLE txm_bitx_kofax_staging.kofax_ub04
ADD COLUMN procedures_other_code_D VARCHAR(7) AFTER procedures_other_date_C; 

ALTER TABLE txm_bitx_kofax_staging.kofax_ub04
ADD COLUMN procedures_other_date_D DATE AFTER procedures_other_code_D; 

ALTER TABLE txm_bitx_kofax_staging.kofax_ub04
ADD COLUMN procedures_other_code_E VARCHAR(7) AFTER procedures_other_date_D; 

ALTER TABLE txm_bitx_kofax_staging.kofax_ub04
ADD COLUMN procedures_other_date_E DATE AFTER procedures_other_code_E;

-- modify cols
ALTER TABLE txm_bitx.provider 
MODIFY COLUMN attending_provider_other_id VARCHAR(30);