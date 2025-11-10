DROP TABLE IF EXISTS txm_preauth_logs.batch_preauth_master;

CREATE TABLE  txm_preauth_logs.batch_preauth_master (
	batch_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
	reprocess_flag char(1) DEFAULT 'N', 
	failed_batch_id int ,
	provider VARCHAR(100) NOT NULL,
	file_type	VARCHAR(100),
	file_name VARCHAR(100),
	file_path	VARCHAR(255),
	data_pipeline_id	VARCHAR(255),
	status	VARCHAR(50) NOT NULL,
	start_datetime	DATETIME(6)  NOT NULL,
	end_datetime	DATETIME(6),
	create_user	VARCHAR(50)  NOT NULL,
	create_timestamp	DATETIME(6)  NOT NULL,
	update_user	VARCHAR(50)  NOT NULL,
	update_timestamp	DATETIME(6)  NOT NULL
) ;

