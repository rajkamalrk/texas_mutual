ALTER TABLE txm_bitx_kofax_staging.kofax_ub04_line 
ADD COLUMN modifier1 VARCHAR(2) AFTER total_charge,
ADD COLUMN modifier2 VARCHAR(2) AFTER modifier1,
ADD COLUMN modifier3 VARCHAR(2) AFTER modifier2,
ADD COLUMN modifier4 VARCHAR(4) AFTER modifier3;

ALTER TABLE txm_bitx_kofax_staging.kofax_ub04 ADD COLUMN creation_date DATE AFTER receive_date;
