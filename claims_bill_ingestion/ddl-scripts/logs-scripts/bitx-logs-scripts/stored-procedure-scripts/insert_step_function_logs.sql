DROP PROCEDURE IF EXISTS txm_bitx_logs.insert_step_function_logs;

DELIMITER $$
CREATE PROCEDURE txm_bitx_logs.insert_step_function_logs (
	IN p_source VARCHAR(50),
    IN p_trading_partner VARCHAR(50),
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
    IN p_reprocess_flag VARCHAR(1),
    IN p_create_user VARCHAR(50),
    IN p_update_user VARCHAR(50),
    OUT p_sf_batch_id INT
)
BEGIN
    DECLARE message VARCHAR(255); 
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        GET DIAGNOSTICS CONDITION 1 @sqlstate = RETURNED_SQLSTATE, @errno = MYSQL_ERRNO, @text = MESSAGE_TEXT;
        SET message = CONCAT('ProcedureExecutionError: ', @errno, ' - ', @text);
        SELECT message AS log_info;
    END;

	SET message = CONCAT('Inserting to staging logs db table txm_bitx_logs.batch_step_function_master ...');
    SELECT message AS log_info;
        INSERT INTO txm_bitx_logs.batch_step_function_master (source
													,trading_partner
													,file_header_id
													,file_name
													,attachment_file_name
													,invoke_payload
													,staging_data_pipeline_id
													,staging_payload
													,enhanced_data_pipeline_id
													,enhanced_payload
													,enrichment_data_pipeline_id
													,enrichment_payload
													,status_type
													,status
													,reprocess_flag
													,start_datetime
													,end_datetime
													,update_timestamp
													,create_user
													,update_user)
												
		VALUES(p_source
			  ,p_trading_partner
			  ,p_file_header_id
			  ,p_file_name
			  ,p_attachment_file_name
			  ,p_invoke_payload
			  ,p_staging_data_pipeline_id
			  ,p_staging_payload
			  ,p_enhanced_data_pipeline_id
			  ,p_enhanced_payload
			  ,p_enrichment_data_pipeline_id
			  ,p_enrichment_payload
			  ,p_status_type
			  ,'IN PROGRESS'
			  ,p_reprocess_flag
			  ,NOW(6)
			  ,NULL
			  ,NOW(6)
			  ,p_create_user
			  ,p_update_user);
	SELECT LAST_INSERT_ID() INTO p_sf_batch_id;
	SET message = CONCAT('SF_BATCH_ID::', p_sf_batch_id);
    SELECT message AS log_info;
    SELECT 'Insert Completed!!!' AS log_info;

END $$
DELIMITER ;