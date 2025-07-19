// Set the roles, warehouses and databases

USE ROLE ACCOUNTADMIN;

USE WAREHOUSE COMPUTE_WH; 

USE SCHEMA MYDB.MYSCHEMA;

// TABLES 

// Create a permanent table. The max retention = 90 day. 

CREATE OR REPLACE TABLE PERMANENT_TABLE
(
ID INT,
NAME STRING
);

ALTER TABLE PERMANENT_TABLE SET DATA_RETENTION_TIME_IN_DAYS = 90; 

// Create a transient table. The max retention = 1 day.

CREATE OR REPLACE TRANSIENT TABLE TRANSIENT_TABLE 
(
ID INT,
NAME STRING
);

// Create a temporary table. The max retention = 1 day.

CREATE OR REPLACE TEMPORARY TABLE TEMPORARY_TABLE 
(
ID INT,
NAME STRING
);

// Type of tables you have created

SHOW TABLES; 

// VIEWS 

// Create an employee table 

CREATE OR REPLACE TABLE employees (
id INTEGER, 
name VARCHAR(50),
department VARCHAR(50),
salary INTEGER
); 

// Insert data into the table 

INSERT INTO employees (id, name, department, salary)
VALUES (1, 'Pat Fay', 'HR', 50000),
       (2, 'Donald OConnell', 'IT', 75000),
       (3, 'Steven King', 'Sales', 60000),
       (4, 'Susan Marvis', 'IT', 80000),
       (5, 'Jennifer Whalen', 'Marketing', 55000); 

// Select data from the table 

SELECT * FROM employees; 


// Let's create a (standard) view called "it_employees" that only includes the employees from the IT department

CREATE OR REPLACE VIEW it_employees AS
SELECT id, name, salary
FROM employees
WHERE department = 'IT'; 

// Select the data from the it_employees view

SELECT * FROM it_employees; 

// Let's create a (secure) view called "hr_employees" that only includes the employees from the HR department

CREATE OR REPLACE  SECURE VIEW hr_employees AS
SELECT id, name, salary
FROM employees
WHERE department = 'HR'; 

// Select the data from the hr_employees view

SELECT * FROM hr_employees; 


// Create a (standard) view that aggregates the salaries by department

CREATE OR REPLACE VIEW employee_salaries
AS SELECT
department,
SUM(salary) AS total_salary
FROM employees
GROUP BY department; 

// Select the data from the employee_salaries view
SELECT * FROM employee_salaries; 

// Create a (materialized) view that aggregates the salaries by department. 
// Snowflake does the aggregation on the back-end, store the data into cache 
// and every time when you are running a select is going to pull that 
// data from that particular cache. 

CREATE OR REPLACE MATERIALIZED VIEW materialized_employee_salaries
AS SELECT
department,
SUM(salary) AS total_salary
FROM employees
GROUP BY department; 

// Select the data from the employee_salaries view
SELECT * FROM materialized_employee_salaries; 

// STAGES

// Create an employee table

CREATE OR REPLACE TABLE customer (
id INTEGER,
name VARCHAR(50),
age INTEGER,
state VARCHAR(50)
);

// Access the table stage 

list @%customer;

// Access the user stage 

list @~;

// Create a named stage (Then drop the csv file using the Snowflake UI)

CREATE OR REPLACE STAGE CUSTOMER_STAGE; 


// Access the names internal stage 

list @CUSTOMER_STAGE;

// Truncate the customer table. 
// Truncate --> Elimina todos los registros de la tabla empleados

TRUNCATE TABLE customer; 

// Select data from customer table 

SELECT * FROM customer;

// Tras eliminar todos los registros, los importamos desde CUSTOMER_STAGE
// Load data from customer table 

copy into customer
from @CUSTOMER_STAGE
file_format = (TYPE='CSV' SKIP_HEADER = 1);

// Select data from customer table 

SELECT * FROM customer;

// FILE FORMAT 

// Create a student table 

CREATE OR REPLACE TABLE student (
id INTEGER,
name VARCHAR(50),
age INTEGER,
marks INTEGER); 

// Create a named stage 
CREATE OR REPLACE STAGE STUDENT_STAGE;

// Access the names internal stage 

list @STUDENT_STAGE;

// Load data from student table 

copy into student
from @STUDENT_STAGE
file_format = (TYPE='CSV' SKIP_HEADER = 1);

// Select data from student table 

SELECT * FROM student;

// Truncate the student table. 

TRUNCATE TABLE student;

// Create a CSV file format

CREATE OR REPLACE FILE FORMAT CSV_FORMAT
TYPE = 'CSV'
FIELD_DELIMITER = ','
RECORD_DELIMITER = '\n'
SKIP_HEADER = 1; 

// Load data from student table with file format

copy into student
from @STUDENT_STAGE
file_format = (FORMAT_NAME = CSV_FORMAT);

// Select data from student table 

SELECT * FROM student;

// Create a JSON file format 

CREATE FILE FORMAT JSON_FORMAT
TYPE = 'JSON';

// Show file formats in your schema

SHOW FILE FORMATS; 

// This is how we are going to use the external stage in Snowflake to access data files into external data storage. 

// STORAGE INTEGRATION WITH S3

// Create a table called USER
// En esta tabla vamos a cargar los datos del archivo subido a S3 bucket

CREATE OR REPLACE TABLE USER (
id INTEGER,
name VARCHAR(50),
location VARCHAR(50),
email VARCHAR(50)
); 

// Create a storage integration with S3 (ir a AWS management console) and IAM role

CREATE OR REPLACE STORAGE INTEGRATION s3_int
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::433154991729:role/snowflakerole'
STORAGE_ALLOWED_LOCATIONS = ('s3://de-academy-training-bucket-3/');


// Describe the storage integration 
DESC INTEGRATION s3_int; 

// Once we set up the relationship between Snowflake and AWS (STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID) we need to create a file format: CSV. 

CREATE OR REPLACE FILE FORMAT my_csv_format
TYPE = 'CSV'
FIELD_DELIMITER = ','
RECORD_DELIMITER = '\n'
SKIP_HEADER = 1; 

// Create a external s3 stage

CREATE OR REPLACE STAGE my_s3_stage
STORAGE_INTEGRATION = s3_int 
URL = 's3://de-academy-training-bucket-3/'
FILE_FORMAT = my_csv_format; 

// Access the external stage 
list @my_s3_stage; 

// Load data to user table without the file format
copy into user
from @my_s3_stage
FILE_FORMAT = (FORMAT_NAME = CSV_FORMAT); 

// Select data from user table 
SELECT * FROM user;

// This is how we are going to use the external stage in Snowflake to access data files into external data storage. 

// CONTINUOUS DATA LOADING --> SNOWPIPE (AUTO-INGEST)

// Create an event table

CREATE OR REPLACE TABLE EVENT (
EVENT VARIANT
);

// Create a storage integration with S3 (ir a AWS management console) and IAM role

CREATE OR REPLACE STORAGE INTEGRATION s3_snowpipe_int
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::433154991729:role/snowpiperol'
STORAGE_ALLOWED_LOCATIONS = ('s3://de-academy-training-bucket-3/event/');

// Once the storage integration is created I need to set up a relationship between snowflake and AWS account.
// Describe the storage integration. It gives us the values of the storage integration. 

DESC INTEGRATION s3_snowpipe_int; 

// Create a file format 

CREATE OR REPLACE FILE FORMAT my_json_format
TYPE = 'JSON'; 

// Create a external s3 stage

CREATE OR REPLACE STAGE my_s3_snowpipe_stage
STORAGE_INTEGRATION = s3_snowpipe_int 
URL = 's3://de-academy-training-bucket-3/event/'
FILE_FORMAT = my_json_format; 

// Access the external stage 

list @my_s3_snowpipe_stage;

// Create a snowpipe to load the event data from s3
CREATE OR REPLACE PIPE s3_pipe
auto_ingest = TRUE AS
COPY INTO EVENT
FROM @my_s3_snowpipe_stage
FILE_FORMAT = (FORMAT_NAME = my_json_format);

// Select the status of the pipe

SELECT SYSTEM$PIPE_STATUS('s3_pipe');

// Once we created the pipe, we need to set up the event notification in our S3 bucket from where the pipe is going to get a notification whenever we upload a new file.  

// Get the notification channel
SHOW PIPES; 

// We upload JSON files into our bucket

// Select data from table 

SELECT * FROM EVENT; 

// After uploading the JSON files to our S3 bucket the pipe has automatically loaded the event.

// CONTINUOUS DATA LOADING --> SNOWPIPE (AUTO-INGEST)

// STREAMS

// ===== Standard Stream =====

// Create a source table 

CREATE OR REPLACE TABLE source_table1 (
id INT,
name VARCHAR,
created_date DATE
); 

// Insert some records 
INSERT INTO source_table1 VALUES 
    (1, 'Chaos', '2023-12-11'),
    (2, 'Genius', '2023-12-11');

// Create a standard stream on the table 

CREATE OR REPLACE STREAM standard_stream ON TABLE source_table1; 

// Select data from table

SELECT * FROM source_table1; 

// Select data from the standard stream

SELECT * FROM standard_stream;

// Insert new values in 'source_table1'

INSERT INTO source_table1 VALUES (3, 'Johnny', '2023-12-11'); 

// After making some changes in 'source_table1' , we select data from the standard stream.

SELECT * FROM standard_stream; 

// Delete a row from 'source_table1'

DELETE FROM source_table1 WHERE id = 2; 

// After making some changes in 'source_table1' , we select data from the standard stream.

SELECT * FROM standard_stream; 

// Update the source table 

UPDATE source_table1 SET name = 'Elon' WHERE id = 1; 

// After making some changes in 'source_table1' , we select data from the standard stream.

SELECT * FROM standard_stream; 

// ===== Append Only Stream =====
// An Append-Only Stream in Snowflake tracks only inserted rows on a table. Unlike a standard stream, it does not track updates or deletes, just new rows appended to the table. Only focusing on catching any new record inserted into your source table.  

CREATE OR REPLACE TABLE source_table2 (
id INT,
name VARCHAR,
created_date DATE
);

// Insert some records 
INSERT INTO source_table2 VALUES 
    (1, 'Chaos', '2023-12-11'),
    (2, 'Genius', '2023-12-11');

// Create a Append Only Stream on the table 

CREATE OR REPLACE STREAM append_only_stream ON TABLE source_table2 APPEND_ONLY = TRUE; 

// Select data from the Append Only Stream

SELECT * FROM append_only_stream;

INSERT INTO source_table2 VALUES (3, 'Johnny', '2023-12-11'); 

// Select data from table

SELECT * FROM source_table2; 

// Select data from the Append Only Stream

SELECT * FROM append_only_stream;

// Update the table.  

UPDATE source_table2 SET name = 'Elon' WHERE id = 1;

// Select data from the Append Only Stream.
// After this update, we see that 'append_only_stream' only catches INSERT, not UPDATE DML (Data Manipulation Language) operation. 

SELECT * FROM append_only_stream;

// ===== How do we use Stream in ETL process =====

CREATE OR REPLACE TABLE TARGET_TABLE2 (
id INT,
name VARCHAR, 
created_date DATE
); 

// Select data from the Append Only Stream

SELECT * FROM append_only_stream;

// We are goint to move the data from 'source_table2' to 'TARGET_TABLE2'

INSERT INTO TARGET_TABLE2
SELECT id, name, created_date FROM append_only_stream; 

// Now we select the data from 'TARGET_TABLE2'

SELECT * FROM TARGET_TABLE2; 

// But when you go back to your Stream and you select it you should not be able to see any records now because since there is a DML operation happen on your Stream where you have access the data out of a Stream and put it into a table. In conclusion, the data has move from 'append_only_stream' to your target table 'TARGET_TABLE2'. This way we avoid duplicate data into your table. 

SELECT * FROM append_only_stream;

INSERT INTO source_table2 VALUES (4, 'Rock', '2023-12-11');

// Select data from table

SELECT * FROM source_table2; 

// We see only 1 record: the DML Operation INSERT. 

SELECT * FROM append_only_stream;

// We are goint to move the data from 'source_table2' to 'TARGET_TABLE2'

INSERT INTO TARGET_TABLE2
SELECT id, name, created_date FROM append_only_stream;

// But when you go back to your Stream and you select it you should not be able to see any records now because since there is a DML operation happen on your Stream where you have access the data out of a Stream and put it into a table. In conclusion, the data has move from 'append_only_stream' to your target table 'TARGET_TABLE2'. This way we avoid duplicate data into your table.

SELECT * FROM append_only_stream;

// Now we select the data from 'TARGET_TABLE2'

SELECT * FROM TARGET_TABLE2; 

// ===== Insert only Stream =====

CREATE EXTERNAL TABLE ext_table 
LOCATION = @MY_AWS_STAGE
FILE_FORMAT = my_format; 

CREATE STREAM my_ext_stream
ON EXTERNAL TABLE ext_table
INSERT_ONLY = TRUE; 

// STREAMS 

// TASKS

// ===== Without a task =====

// Create a source table 
CREATE OR REPLACE TABLE SOURCE_TABLE (
id INT,
name VARCHAR, 
created_date DATE
); 

// Insert some records on the source table
INSERT INTO SOURCE_TABLE VALUES 
    (1, 'Chaos', '2023-12-11'),
    (2, 'Genius', '2024-07-04'); 

// Select data from source table 
SELECT * FROM SOURCE_TABLE; 

// Create a target table 
CREATE OR REPLACE TABLE TARGET_TABLE (
id INT, 
name VARCHAR, 
created_date DATE, 
created_day VARCHAR,
created_month VARCHAR,
created_year VARCHAR
); 

// Insert data from source table into the target table 
INSERT INTO TARGET_TABLE
SELECT
a.id,
a.name,
a.created_date,
DAY(a.created_date) AS created_day,
MONTH(a.created_date) AS created_month,
YEAR(a.created_date) AS created_year
FROM SOURCE_TABLE a
LEFT JOIN TARGET_TABLE b
ON a.id = b.id
WHERE b.id IS NULL; 

// Select data from target table
SELECT * FROM TARGET_TABLE; 

// Insert some records on the source table 
INSERT INTO SOURCE_TABLE VALUES 
(3, 'Elan', '2022-02-24'); 

// ===== Without a task =====

// ===== With a task =====

// Create a source table 
CREATE OR REPLACE TABLE SOURCE_TABLE (
id INT,
name VARCHAR, 
created_date DATE
); 

// Insert some records on the source table
INSERT INTO SOURCE_TABLE VALUES 
    (1, 'Chaos', '2023-12-11'),
    (2, 'Genius', '2024-07-04'); 

// Select data from source table 
SELECT * FROM SOURCE_TABLE; 

// Create a target table 
CREATE OR REPLACE TABLE TARGET_TABLE (
id INT, 
name VARCHAR, 
created_date DATE, 
created_day VARCHAR,
created_month VARCHAR,
created_year VARCHAR
); 

// Select from the target table 
SELECT * FROM TARGET_TABLE;

// Insert data from source table into the target table through a TASK. 
CREATE OR REPLACE TASK my_task
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 MINUTE'
AS
INSERT INTO TARGET_TABLE
SELECT
a.id,
a.name,
a.created_date,
DAY(a.created_date) AS created_day,
MONTH(a.created_date) AS created_month,
YEAR(a.created_date) AS created_year
FROM SOURCE_TABLE a
LEFT JOIN TARGET_TABLE b
ON a.id = b.id
WHERE b.id IS NULL;

// Select from the target table 
SELECT * FROM TARGET_TABLE;

// Show tasks (Firstly the task is suspended)
SHOW TASKS; 

//Alter the task to execute 
ALTER TASK my_task RESUME; 

//Check the task status 
SELECT * FROM TABLE (information_schema.TASK_HISTORY(TASK_NAME => 'my_task')); 

// Select from the target table 
SELECT * FROM TARGET_TABLE;

// Insert some records on the source table. In 1 minute the task will insert from the source table to the target table. 
INSERT INTO SOURCE_TABLE VALUES 
(3, 'Elan', '2022-02-24'); 

// Select from the target table 
SELECT * FROM TARGET_TABLE;

// This is how we can automate any execution of any SQL query. 

// ===== With a task =====

// TASKS

// TIME TRAVEL

// ===== Time travel (deleting some rows) =====

CREATE OR REPLACE TABLE DROP_TABLE 
(
ID INT,
NAME VARCHAR
);

INSERT INTO DROP_TABLE VALUES 
(1, 'JOHN'),
(2, 'SAM'),
(3, 'ELAN'),
(4, 'MARK');

SELECT * FROM DROP_TABLE; 

DELETE FROM DROP_TABLE WHERE ID = 4; 

// Check the table creation time
SELECT CREATED FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'DROP_TABLE';

// Select the data in my table 60 seconds ago. This attempts to access the state of the table 60 seconds ago
SELECT * FROM DROP_TABLE AT (OFFSET => -60); 

// Check the status of the DROP_TABLE table at a specific point in time in the past. 10 seconds after creating it, continue having 4 records
SELECT * FROM DROP_TABLE AT (TIMESTAMP => '2025-07-16 04:59:45.608 -0700'::timestamp_tz);

// This query uses Time Travel in Snowflake to retrieve the state of the DROP_TABLE table just before the statement with the specified ID (query history) was executed. Jsut before droping one record, there were 4 records. 
SELECT * FROM DROP_TABLE BEFORE(STATEMENT => '01bdb9ef-0001-6137-0001-58fa000258e2');

// Create a table with the data before deleting one record. 
CREATE OR REPLACE TABLE DROP_TABLE_CLONE AS
SELECT * FROM DROP_TABLE BEFORE(STATEMENT => '01bdb9ef-0001-6137-0001-58fa000258e2');

SELECT * FROM DROP_TABLE_CLONE;

TRUNCATE TABLE DROP_TABLE; 

INSERT INTO DROP_TABLE
SELECT * FROM DROP_TABLE_CLONE; 

DROP TABLE DROP_TABLE_CLONE;

SELECT * FROM DROP_TABLE; 

// ===== Time travel (deleting some rows) =====

// ===== Dropping table, schema and database =====

DROP TABLE DROP_TABLE;

SELECT * FROM DROP_TABLE;

UNDROP TABLE DROP_TABLE; 

DROP SCHEMA MYSCHEMA; 

SELECT * FROM MYSCHEMA;

UNDROP SCHEMA MYSCHEMA; 

DROP DATABASE MYDB; 

SELECT * FROM MYDB;

UNDROP DATABASE MYDB; 

USE SCHEMA MYDB.MYSCHEMA;

SHOW DATABASES;

SHOW SCHEMAS; 

SHOW TABLES; 

// ===== Dropping table, schema and database =====

// TIME TRAVEL

// CLONING

CREATE OR REPLACE DATABASE test_mydb
CLONE mydb; 

DROP DATABASE test_mydb; 

// CLONING






























