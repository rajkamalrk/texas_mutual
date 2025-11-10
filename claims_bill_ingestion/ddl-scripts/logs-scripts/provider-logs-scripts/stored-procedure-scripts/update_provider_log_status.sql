DROP PROCEDURE IF EXISTS txm_bitx_provider_logs.update_provider_log_status;

DELIMITER $$
CREATE PROCEDURE txm_bitx_provider_logs.update_provider_log_status(
    IN p_batch_id INT,
    IN p_status VARCHAR(50),
	IN p_error_code VARCHAR(100),
    IN p_error_type VARCHAR(100),
    IN p_error_message VARCHAR(4000),
    IN p_create_user VARCHAR(50),
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
    
	SET message = CONCAT('Updating provider logs db table txm_bitx_provider_logs.batch_provider_details for BATCH_ID::', p_batch_id, '...');
    SELECT message AS log_info;
    UPDATE txm_bitx_provider_logs.batch_provider_details
    SET status = p_status
       ,end_datetime = NOW(6)
       ,update_timestamp = NOW(6)
       ,update_user = p_update_user
	WHERE batch_id = p_batch_id;
    SELECT 'Update Completed!!!' AS log_info;

    SET message = CONCAT('Checking input p_status::', p_status, '...');
	SELECT message AS log_info;

    IF UPPER(p_status) = 'FAILED'
    THEN
	  SET message = CONCAT('Inserting to provider logs db table txm_bitx_provider_logs.batch_provider_error_details...');
      SELECT message AS log_info;
      
      INSERT INTO txm_bitx_provider_logs.batch_provider_error_details (batch_id
														    ,error_code
														    ,error_type
														    ,error_message
														    ,create_timestamp
														    ,update_timestamp
														    ,create_user
														    ,update_user)
	    VALUES(p_batch_id
		      ,p_error_code
			  ,p_error_type
			  ,p_error_message
			  ,NOW(6)
			  ,NOW(6)
			  ,p_create_user
			  ,p_update_user);
	  
	  SELECT 'Insert Completed!!!' AS log_info;
	END IF;

END$$
DELIMITER ;
