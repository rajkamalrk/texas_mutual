DROP PROCEDURE IF EXISTS txm_bitx_logs.update_step_function_failure_log_status;

DELIMITER $$
CREATE PROCEDURE txm_bitx_logs.update_step_function_failure_log_status (
    IN p_sf_batch_id INT,
    IN p_job_type VARCHAR(100),
	IN p_job_name VARCHAR(255),
	IN p_job_id VARCHAR(255),
	IN p_payload VARCHAR(1000),
    IN p_status VARCHAR(50),
	IN p_error_code VARCHAR(100),
    IN p_error_info VARCHAR(100),
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
    
	SET message = CONCAT('Updating staging logs db table txm_bitx_logs.batch_step_function_master for SF_BATCH_ID::', p_sf_batch_id, '...');
    SELECT message AS log_info;
    UPDATE txm_bitx_logs.batch_step_function_master
    SET status = p_status
       ,end_datetime = NOW(6)
       ,update_timestamp = NOW(6)
       ,update_user = p_update_user
	WHERE sf_batch_id = p_sf_batch_id;
    SELECT 'Update Completed!!!' AS log_info;

    SET message = CONCAT('Checking input p_status::', p_status, '...');
	SELECT message AS log_info;

    IF UPPER(p_status) LIKE 'FAILED%'
    THEN
	  SET message = CONCAT('Inserting to staging logs db table txm_bitx_logs.batch_step_function_error_details...');
      SELECT message AS log_info;
      
      INSERT INTO txm_bitx_logs.batch_step_function_error_details (sf_batch_id
																,job_type
																,job_name
																,job_id
																,payload
																,error_code
																,error_info
																,error_message
																,create_timestamp
																,update_timestamp
																,create_user
																,update_user)
	    VALUES(p_sf_batch_id
			  ,p_job_type
			  ,p_job_name
			  ,p_job_id
			  ,p_payload
		      ,p_error_code
			  ,p_error_info
			  ,p_error_message
			  ,NOW(6)
			  ,NOW(6)
			  ,p_create_user
			  ,p_update_user);
	  
	  SELECT 'Insert Completed!!!' AS log_info;
	END IF;

END $$
DELIMITER ;