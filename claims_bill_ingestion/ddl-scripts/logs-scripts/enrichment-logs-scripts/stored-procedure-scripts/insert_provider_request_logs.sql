DROP PROCEDURE IF EXISTS txm_bitx_enrichment_logs.insert_provider_request_logs;
DELIMITER $$
CREATE PROCEDURE txm_bitx_enrichment_logs.insert_provider_request_logs(
	IN p_source VARCHAR(25),
    IN p_trading_partner VARCHAR(50),
    IN p_file_header_id VARCHAR(50),
    IN p_job_id VARCHAR(255),
    IN p_create_user VARCHAR(50),
    IN p_update_user VARCHAR(50),
    OUT p_batch_id INT
)
BEGIN
    DECLARE message VARCHAR(255); 
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        GET DIAGNOSTICS CONDITION 1 @sqlstate = RETURNED_SQLSTATE, @errno = MYSQL_ERRNO, @text = MESSAGE_TEXT;
        SET message = CONCAT('ProcedureExecutionError: ', @errno, ' - ', @text);
        SELECT message AS log_info;
    END;

	SET message = CONCAT('Inserting to enrichment logs db table txm_bitx_enrichment_logs.batch_provider_request_master...');
    SELECT message AS log_info;
        INSERT INTO txm_bitx_enrichment_logs.batch_provider_request_master (source
		                                            ,trading_partner
	                                                ,file_header_id
													,job_id
													,status
													,start_datetime
													,end_datetime
													,update_timestamp
													,create_user
													,update_user)
												
		VALUES(p_source
			  ,p_trading_partner
		      ,p_file_header_id
			  ,p_job_id
			  ,'IN PROGRESS'
			  ,NOW(6)
			  ,NULL
			  ,NOW(6)
			  ,p_create_user
			  ,p_update_user);
	SELECT LAST_INSERT_ID() INTO p_batch_id;
	SET message = CONCAT('BATCH_ID::', p_batch_id);
    SELECT message AS log_info;
    SELECT 'Insert Completed!!!' AS log_info;

END$$
DELIMITER ;