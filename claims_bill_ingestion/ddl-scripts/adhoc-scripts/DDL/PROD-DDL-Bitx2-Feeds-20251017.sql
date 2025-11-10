-- adding new cols
ALTER TABLE txm_bitx.billing_documents
ADD COLUMN is_active VARCHAR(1) DEFAULT 'Y' AFTER file_type;