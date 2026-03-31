---External stages
--CONNECT AWS to snowflake
---Storage Integration
--Role
--Buckets



CREATE DATABASE testing;

CREATE TABLE customers (
customer_id INT,
name VARCHAR,
city VARCHAR
);

CREATE OR REPLACE FILE FORMAT csv_format
TYPE = CSV
FIELD_DELIMITER = ','
SKIP_HEADER = 1;

CREATE STAGE EXT_STAGE
URL = 's3://ubaid-testing/sourcefolder/';



SHOW STAGES;


LIST @EXT_STAGE;

COPY INTO customers
FROM @EXT_STAGE
FILE_FORMAT = my_csv_format
-- credentials=(aws_key_id='AKIAVRUVVWHJN2O4LEN3' aws_secret_key='7p18WnfurXGXTdeZGBs7fqIURRVUbQOTEFqcLRZ0');



CREATE OR REPLACE STAGE EXXT_STAGE
URL='s3://ubaid-testing/sourcefolder/'
FILE_FORMAT = csv_format
-- CREDENTIALS=(aws_key_id='AKIAVRUVVWHJN2O4LEN3' aws_secret_key='7p18WnfurXGXTdeZGBs7fqIURRVUbQOTEFqcLRZ0');


SHOW STAGES;

LIST @EXXT_STAGE;

COPY INTO customers
FROM @EXXT_STAGE
-- CREDENTIALS=(aws_key_id='AKIAVRUVVWHJN2O4LEN3' aws_secret_key='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');


SHOW STAGES;

DROP STAGE EXT_STAGE;

DROP STAGE EXXT_STAGE;


CREATE STAGE EXT_STAGE
URL = 's3://ubaid-testing/sourcefolder/';

SHOW STAGES;

SHOW FILE FORMATS;
DROP FILE FORMAT my_csv_format;

DROP FILE FORMAT csv_format;


CREATE FILE FORMAT my_csv_format
TYPE = CSV
FIELD_DELIMITER = ','
SKIP_HEADER = 1;


COPY INTO customers
FROM @EXT_STAGE
FILE_FORMAT = my_csv_format
-- CREDENTIALS=(AWS_KEY_ID='AKIAVRUVVWHJN2O4LEN3' AWS_SECRET_KEY='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');


SELECT * FROM customers;


CREATE STORAGE INTEGRATION S3_int
TYPE= EXTERNAL_STAGE
Storage_provider=S3
enabled=TRUE
-- Storage_aws_role_arn='arn:aws:iam::381492244946:role/AWS_testing'
-- Storage_allowed_locations=('s3://ubaid-testing/sourcefolder/');


DESC INTEGRATION S3_int;

CREATE OR REPLACE STAGE S3_STAGE
FILE_FORMAT = my_csv_format
storage_integration = S3_int
URL = 's3://ubaid-testing/sourcefolder/';


LIST @S3_STAGE;

REMOVE @S3_STAGE;

TRUNCATE TABLE customers;


COPY INTO customers
FROM @S3_STAGE;

SELECT * FROM customers;

----------------------------

--- CREATE THE SNOWPIPE

CREATE PIPE AWSPIPE
AUTO_INGEST = TRUE
AS
COPY INTO customers
FROM @S3_STAGE;


SHOW PIPES;

SELECT COUNT(*) FROM customers;

SELECT SYSTEM$PIPE_STATUS('AWSPIPE');


SELECT * FROM customers;

SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY WHERE PIPE_NAME='AWSPIPE';


--how to check credits used for any snowpipe

SELECT *
FROm table(information_schema.pipe_usage_history(
date_range_start=>dateadd('hour',-12,current_timestamp()),
pipe_name=>'AWSPIPE'));



