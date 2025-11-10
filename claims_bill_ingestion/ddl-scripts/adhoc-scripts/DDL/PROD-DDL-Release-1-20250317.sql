ALTER TABLE txm_bitx_kofax_staging.kofax_dwc066_attachments DROP INDEX serial_number;
ALTER TABLE txm_bitx_kofax_staging.kofax_cms1500_attachments DROP INDEX serial_number;
ALTER TABLE txm_bitx_kofax_staging.kofax_ub04_attachments DROP INDEX serial_number;

ALTER TABLE txm_bitx.hospitalization_treatment_line MODIFY COLUMN units_of_service decimal(10,2);
ALTER TABLE txm_bitx.professional_treatment_line MODIFY COLUMN units decimal(10,2);

ALTER TABLE txm_bitx.billing_documents MODIFY file_type VARCHAR(20) DEFAULT 'ATTACHMENT';