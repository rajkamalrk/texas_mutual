DROP PROCEDURE IF EXISTS txm_bitx_logs.update_step_function_log_status;

DELIMITER $$
CREATE PROCEDURE txm_bitx_logs.update_step_function_log_status (
    IN p_sf_batch_id INT,
    IN p_file_header_id INT,
    IN p_file_name VARCHAR(100),
    IN p_attachment_file_name VARCHAR(100),
	IN p_invoke_payload VARCHAR(1000),
    IN p_staging_data_pipeline_id VARCHAR(255),
    IN p_staging_payload VARCHAR(1000),
    IN p_enhanced_data_pipeline_id VARCHAR(255),
    IN p_enhanced_payload VARCHAR(1000),
    IN p_enrichment_data_pipeline_id VARCHAR(255),
    IN p_enrichment_payload VARCHAR(1000),
	IN p_status_type VARCHAR(100),
	IN p_status VARCHAR(100),
    IN p_update_user VARCHAR(50)
)
BEGIN
    DECLARE message VARCHAR(255); 
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        GET DIAGNOSTICS CONDITION 1 @sqlstate = RETURNED_SQLSTATE, @errno = MYSQL_ERRNO, @text = MESSAGE_TEXT;
        SET message = CONCAT('ProcedureExecutionError: ', @errno, ' - ', @text);
        SELECT message AS log_info;
    END;
    
	SET message = CONCAT('Updating staging logs db table txm_bitx_logs.batch_step_function_master for SF_BATCH_ID::', p_sf_batch_id, '...');
    SELECT message AS log_info;
    UPDATE txm_bitx_logs.batch_step_function_master
    SET file_header_id = IFNULL(p_file_header_id, file_header_id)
	   ,file_name = IFNULL(p_file_name, file_name)
	   ,attachment_file_name = IFNULL(p_attachment_file_name, attachment_file_name)
	   ,invoke_payload = IFNULL(p_invoke_payload, invoke_payload)
	   ,staging_data_pipeline_id = IFNULL(p_staging_data_pipeline_id, staging_data_pipeline_id)
	   ,staging_payload = IFNULL(p_staging_payload, staging_payload)
	   ,enhanced_data_pipeline_id = IFNULL(p_enhanced_data_pipeline_id, enhanced_data_pipeline_id)
	   ,enhanced_payload = IFNULL(p_enhanced_payload, enhanced_payload)
	   ,enrichment_data_pipeline_id = IFNULL(p_enrichment_data_pipeline_id, enrichment_data_pipeline_id)
	   ,enrichment_payload = IFNULL(p_enrichment_payload, enrichment_payload)
	   ,status_type = IFNULL(p_status_type, status_type)
       ,status = IFNULL(p_status, status)
       ,end_datetime = NOW(6)
       ,update_timestamp = NOW(6)
       ,update_user = p_update_user
	WHERE sf_batch_id = p_sf_batch_id;
    SELECT 'Update Completed!!!' AS log_info;
END $$
DELIMITER ;
