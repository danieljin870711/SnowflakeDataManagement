
# Snowflake Data Management

In this project, I will demonstrate how to manage data warehouse as a role of account admin using Snowflake. The scope of this project is listed as follow:

## Project Covers...

- Loading data

- Loading unstructured data

- Performance optimization

- Zero copy cloning

- Data sharing

- Data sampling

- Scheduling tasks

- Streams

- Materialized views

- Access Management 

- Connecting to Power BI & Tableau


### Loading data

In this project, I will load data from the public AWS S3 bucket into a data warehouse in Snowflake using COPY command. 

To do so, let's create a database and schema first. 

```http
CREATE OR REPLACE DATABASE MANAGE_DB;

CREATE OR REPLACE SCHEMA external_stages;
```

I am going to create aws_stage table in the stage schema location to extract files from the public AWS S3 bucket using the following credential. 
```http
CREATE OR REPLACE STAGE MANAGE_DB.external_stages.aws_stage
    url='s3://bucketsnowflakes3'
    credentials=(aws_key_id='ABCD_DUMMY_ID' aws_secret_key='1234abcd_key');
```

Check properties of the files in the external stage using DESC function. 
```http
DESC STAGE MANAGE_DB.external_stages.aws_stage; 
```

![App Screenshot](https://raw.githubusercontent.com/danieljin870711/SnowflakeDataManagement/fd273b9d568c1b5afe47187a2ed435bcb937d9e3/Screenshots/1..png)

We can also view the list of the files using LIST command. When we call stage schema, we need to add '@' in front of the name. 

```http
LIST @aws_stage;
```
![App Screenshot](https://github.com/danieljin870711/SnowflakeDataManagement/blob/main/Screenshots/2.png?raw=true)

We see three files in the stage table. Let's add 'OrderDetails' csv file into 'Orders' table in the Public schema. 

```http
COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS
    FROM @MANAGE_DB.external_stages.aws_stage
    file_format= (type = csv field_delimiter=',' skip_header=1)
    files = ('OrderDetails.csv');
```

Or you can use PATTERN command to select a file containing 'Order'. 
```http
COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS
    FROM @aws_stage
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*';

SELECT *
FROM ORDERS;
```
And let's confirm the content of 'OrderDetails' file by executing the Order table. 

![App Screenshot](https://github.com/danieljin870711/SnowflakeDataManagement/blob/main/Screenshots/3.png?raw=true)

### Loading Unstructured data

We can use Snowfalke to load and transform unstructured data, such as JSON format, easily. 

Let's load JSON file from S3 bucket to Snowflake database by creating connection between S3 bucket and external stage schema table. Then use the file format object to load the HR data into the JSON_RAW table. 

``` http
CREATE OR REPLACE stage MANAGE_DB.EXTERNAL_STAGES.JSONSTAGE
     url='s3://bucketsnowflake-jsondemo';

CREATE OR REPLACE SCHEMA FILE_FORMATS;

CREATE OR REPLACE file format MANAGE_DB.FILE_FORMATS.JSONFORMAT
    TYPE = JSON;
    
    
CREATE OR REPLACE table OUR_FIRST_DB.PUBLIC.JSON_RAW (
    raw_file variant);
    
COPY INTO OUR_FIRST_DB.PUBLIC.JSON_RAW
    FROM @MANAGE_DB.EXTERNAL_STAGES.JSONSTAGE
    file_format= MANAGE_DB.FILE_FORMATS.JSONFORMAT
    files = ('HR_data.json');
```

Let's view the content of the JSON file. 
```http
SELECT * FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;
```
![App Screenshot](https://github.com/danieljin870711/SnowflakeDataManagement/blob/main/Screenshots/4.a.png?raw=true)

We can transform the JSON format using alias. As you can see below, the view of the JSON format has been changed to structured data format. 

```http
SELECT 
    RAW_FILE:id::int as id,  
    RAW_FILE:first_name::STRING as first_name,
    RAW_FILE:last_name::STRING as last_name,
    RAW_FILE:gender::STRING as gender

FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;
```

![App Screenshot](https://github.com/danieljin870711/SnowflakeDataManagement/blob/main/Screenshots/5..png?raw=true)

We can create a dataset of JSON format using two methodologies below. 

Option 1. Create table as
```http
CREATE OR REPLACE TABLE Languages AS
select
      RAW_FILE:first_name::STRING as First_name,
    f.value:language::STRING as First_language,
   f.value:level::STRING as Level_spoken
from OUR_FIRST_DB.PUBLIC.JSON_RAW, table(flatten(RAW_FILE:spoken_languages)) f;

```

Option 2. Insert into
```http
INSERT INTO Languages
select
      RAW_FILE:first_name::STRING as First_name,
    f.value:language::STRING as First_language,
   f.value:level::STRING as Level_spoken
from OUR_FIRST_DB.PUBLIC.JSON_RAW, table(flatten(RAW_FILE:spoken_languages)) f;
```

### Performance Optimization 

There are multiple ways to optimize query performance. In this exercise, I will show you two methodologies, creating dedicated virtual warehouse per user group and adding clustering key. 

Below is a query for implementing two data warehouses for D.S and Marketing departments with auto scaling. With this dedicated server per user group and auto scaling of data warehouses, we can optimize query performance. 

```http
// Data Scientists
CREATE WAREHOUSE DS_WH 
WITH WAREHOUSE_SIZE = 'SMALL'
WAREHOUSE_TYPE = 'STANDARD' 
AUTO_SUSPEND = 300 
AUTO_RESUME = TRUE 
MIN_CLUSTER_COUNT = 1 
MAX_CLUSTER_COUNT = 1 
SCALING_POLICY = 'STANDARD';

// Marketing
CREATE WAREHOUSE MARKETING_WH 
WITH WAREHOUSE_SIZE = 'XSMALL'
WAREHOUSE_TYPE = 'STANDARD' 
AUTO_SUSPEND = 300 
AUTO_RESUME = TRUE 
MIN_CLUSTER_COUNT = 1 
MAX_CLUSTER_COUNT = 1 
SCALING_POLICY = 'STANDARD';
```
Another option is to add a cluster key into the column(s) that you call often. By doing so, Snowflake will compartmentalize the column(s) and only query the condition of the cluster mentioned in the where clause. 

To do so, I created ORDERS_CACHING table to test the query performance before and after clustering. 

```http
// Publicly accessible staging area    

CREATE OR REPLACE STAGE MANAGE_DB.external_stages.aws_stage
    url='s3://bucketsnowflakes3';

// List files in stage

LIST @MANAGE_DB.external_stages.aws_stage;

//Load data using copy command

COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS
    FROM @MANAGE_DB.external_stages.aws_stage
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*OrderDetails.*';
    

// Create table

CREATE OR REPLACE TABLE ORDERS_CACHING (
ORDER_ID	VARCHAR(30)
,AMOUNT	NUMBER(38,0)
,PROFIT	NUMBER(38,0)
,QUANTITY	NUMBER(38,0)
,CATEGORY	VARCHAR(30)
,SUBCATEGORY	VARCHAR(30)
,DATE DATE)   ; 


INSERT INTO ORDERS_CACHING 
SELECT
t1.ORDER_ID
,t1.AMOUNT	
,t1.PROFIT	
,t1.QUANTITY	
,t1.CATEGORY	
,t1.SUBCATEGORY	
,DATE(UNIFORM(1500000000,1700000000,(RANDOM())))
FROM ORDERS t1
CROSS JOIN (SELECT * FROM ORDERS) t2
CROSS JOIN (SELECT TOP 100 * FROM ORDERS) t3;
```

Let's see its query result before clustering. 
```http
SELECT * FROM ORDERS_CACHING  WHERE DATE = '2020-06-09';
```
![App Screenshot](https://github.com/danieljin870711/SnowflakeDataManagement/blob/main/Screenshots/6.a.png?raw=true)


Now let's add a cluster key into date column to compare the query result.

```http
ALTER TABLE ORDERS_CACHING CLUSTER BY ( DATE ) ;

SELECT * FROM ORDERS_CACHING  WHERE DATE = '2020-01-05';
```
![App Screenshot](https://github.com/danieljin870711/SnowflakeDataManagement/blob/main/Screenshots/6.b.png?raw=true)

The query performance has been increased from 170ms to 48ms. You will see more distinct difference if you run a complex and heavy query. 


### Zero Copy Cloning 
The function of zero copy cloning is to copy or replicate a database, a schema, a table with metadata with cost saving. As this function is to reference the original data storage with meta data without any additional cost. With this characteristic, this function is great benefit in developing data architecture. 

```http

// Cloning Schema
CREATE TRANSIENT SCHEMA OUR_FIRST_DB.COPIED_SCHEMA
CLONE OUR_FIRST_DB.PUBLIC;

SELECT * FROM COPIED_SCHEMA.CUSTOMERS;


CREATE TRANSIENT SCHEMA OUR_FIRST_DB.EXTERNAL_STAGES_COPIED
CLONE MANAGE_DB.EXTERNAL_STAGES;


// Cloning Database
CREATE TRANSIENT DATABASE OUR_FIRST_DB_COPY
CLONE OUR_FIRST_DB;

DROP DATABASE OUR_FIRST_DB_COPY;
DROP SCHEMA OUR_FIRST_DB.EXTERNAL_STAGES_COPIED;
DROP SCHEMA OUR_FIRST_DB.COPIED_SCHEMA;

// Cloning Table

SELECT * FROM OUR_FIRST_DB.PUBLIC.CUSTOMERS;

CREATE TABLE OUR_FIRST_DB.PUBLIC.CUSTOMERS_CLONE
CLONE OUR_FIRST_DB.PUBLIC.CUSTOMERS;

```

After cloning the database, schema, and table, you can use SWIPE command to switch from dev to prod env. 

```http
ALTER TABLE Table_You_Want_To_Swap_From
SWAP WITH Table_You_Want_To_Swap_To;

```


### Data Sharing 
Thanks to the unique architecture of Snowflake, data sharing internally and externally is simple. The reason is because its compute resources and storages are decoupled and the function of cloning metadata with READ ONLY. 

To do so, let's load a sample data into a 'ORDERS' table from S3 bucket. 

``` http
CREATE OR REPLACE DATABASE DATA_S;


CREATE OR REPLACE STAGE aws_stage
    url='s3://bucketsnowflakes3';

// Create table
CREATE OR REPLACE TABLE ORDERS (
ORDER_ID	VARCHAR(30)
,AMOUNT	NUMBER(38,0)
,PROFIT	NUMBER(38,0)
,QUANTITY	NUMBER(38,0)
,CATEGORY	VARCHAR(30)
,SUBCATEGORY	VARCHAR(30));


// Load data using copy command
COPY INTO ORDERS
    FROM @MANAGE_DB.external_stages.aws_stage
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*OrderDetails.*';
    
SELECT * FROM ORDERS;
```

Then, let's create a share object and setup grants for database, schema, table, and select to access to the Share location. 

```http
// Create a share object
CREATE OR REPLACE SHARE ORDERS_SHARE;

---- Setup Grants ----

// Grant usage on database
GRANT USAGE ON DATABASE DATA_S TO SHARE ORDERS_SHARE; 

// Grant usage on schema
GRANT USAGE ON SCHEMA DATA_S.PUBLIC TO SHARE ORDERS_SHARE; 

// Grant SELECT on table

GRANT SELECT ON TABLE DATA_S.PUBLIC.ORDERS TO SHARE ORDERS_SHARE; 

// Validate Grants
SHOW GRANTS TO SHARE ORDERS_SHARE;
```

Next, let's create a share account so that it can access to the Share object, ORDERS_SHARE. 

```http
CREATE MANAGED ACCOUNT tech_joy_account
ADMIN_NAME = tech_joy_admin,
ADMIN_PASSWORD = 'set-pwd',
TYPE = READER;

// Make sure to have selected the role of accountadmin

// Show accounts
SHOW MANAGED ACCOUNTS;


-- Share the data -- 

ALTER SHARE ORDERS_SHARE 
ADD ACCOUNT = <reader-account-id>;

// Override business share restriction 
ALTER SHARE ORDERS_SHARE 
ADD ACCOUNT =  <reader-account-id>
SHARE_RESTRICTIONS=false;
```

With the grants to database, schema, table, and select clause, we can create a database and virtual warehouse so that the non Snowflake users can access to the Share location. 

```http
// Show all shares (consumer & producers)
SHOW SHARES;

// See details on share
DESC SHARE QNA46172.ORDERS_SHARE;

// Create a database in consumer account using the share
CREATE DATABASE DATA_SHARE_DB FROM SHARE <account_name_producer>.ORDERS_SHARE;

// Validate table access
SELECT * FROM  DATA_SHARE_DB.PUBLIC.ORDERS;


// Setup virtual warehouse
CREATE WAREHOUSE READ_WH WITH
WAREHOUSE_SIZE='X-SMALL'
AUTO_SUSPEND = 180
AUTO_RESUME = TRUE
INITIALLY_SUSPENDED = TRUE;
```

Once the Share object has all previlige to access to database, schema, table, and select as well as it has own database and virtual warehouse, let's setup users for Share location. 

```http
CREATE USER MYRIAM PASSWORD = '1234'

GRANT USAGE ON WAREHOUSE READ_WH TO ROLE PUBLIC;

GRANT IMPORTED PRIVILEGES ON DATABASE DATA_SARE_DB TO ROLE PUBLIC; 
```

In the real business scenario, we would prefer to create their data in VIEW schema, separting them to our DBO schema. Let's see how to create a dataset within a VIEW schema for non Snowflake users. 

First, let's create a database and stage as well as a file format. And load the dataset into the table, 

```http
CREATE OR REPLACE DATABASE CUSTOMER_DB;

CREATE OR REPLACE TABLE CUSTOMER_DB.public.customers (
   id int,
   first_name string,
  last_name string,
  email string,
  gender string,
  Job string,
  Phone string);

    
// Stage and file format
CREATE OR REPLACE FILE FORMAT MANAGE_DB.file_formats.csv_file
    type = csv
    field_delimiter = ','
    skip_header = 1;
    
CREATE OR REPLACE STAGE MANAGE_DB.external_stages.time_travel_stage
    URL = 's3://data-snowflake-fundamentals/time-travel/'
    file_format = MANAGE_DB.file_formats.csv_file;
    
LIST  @MANAGE_DB.external_stages.time_travel_stage;


// Copy data and insert in table
COPY INTO CUSTOMER_DB.public.customers
FROM @MANAGE_DB.external_stages.time_travel_stage
files = ('customers.csv');

SELECT * FROM  CUSTOMER_DB.PUBLIC.CUSTOMERS;
```

After creating the CUSTOMERS table, let's create a normal VIEW and secure VIEW table. I would recommend using a secure view table as it can hide a code so that external business users cannot see the restriction. 

```http
-- Create VIEW -- 
CREATE OR REPLACE VIEW CUSTOMER_DB.PUBLIC.CUSTOMER_VIEW AS
SELECT 
FIRST_NAME,
LAST_NAME,
EMAIL
FROM CUSTOMER_DB.PUBLIC.CUSTOMERS
WHERE JOB != 'DATA SCIENTIST'; 


-- Grant usage & SELECT --
GRANT USAGE ON DATABASE CUSTOMER_DB TO ROLE PUBLIC;
GRANT USAGE ON SCHEMA CUSTOMER_DB.PUBLIC TO ROLE PUBLIC;
GRANT SELECT ON TABLE CUSTOMER_DB.PUBLIC.CUSTOMERS TO ROLE PUBLIC;
GRANT SELECT ON VIEW CUSTOMER_DB.PUBLIC.CUSTOMER_VIEW TO ROLE PUBLIC;


SHOW VIEWS LIKE '%CUSTOMER%';


-- Create SECURE VIEW -- 

CREATE OR REPLACE SECURE VIEW CUSTOMER_DB.PUBLIC.CUSTOMER_VIEW_SECURE AS
SELECT 
FIRST_NAME,
LAST_NAME,
EMAIL
FROM CUSTOMER_DB.PUBLIC.CUSTOMERS
WHERE JOB != 'DATA SCIENTIST' ;

GRANT SELECT ON VIEW CUSTOMER_DB.PUBLIC.CUSTOMER_VIEW_SECURE TO ROLE PUBLIC;

SHOW VIEWS LIKE '%CUSTOMER%';
```

### Data Sampling 
For SQL development, we sometime encounter increasing our computing resources by excuting transaction table multiple times. To remediate overusing the computing resource, we can use data sampling to target a view table contaning sample dataset of the original dataset. 

Let's create a ADDRESS_SAMPLE view table for data sampling using SAMPLE ROW command. 

```http
CREATE OR REPLACE TRANSIENT DATABASE SAMPLING_DB;

CREATE OR REPLACE VIEW ADDRESS_SAMPLE
AS 
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.CUSTOMER_ADDRESS 
SAMPLE ROW (1) SEED(27); -- 1% of dataset

SELECT * FROM ADDRESS_SAMPLE;
```

We have another method to create a vew table for data sampling using SAMPLE SYSTEM command. Instead of randomly selecting the data, Snowflake selects data from each micro partition 

```http
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.CUSTOMER_ADDRESS 
SAMPLE SYSTEM (1) SEED(23); -- micro-partition

SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.CUSTOMER_ADDRESS 
SAMPLE SYSTEM (10) SEED(23); -- 10% of micro partision
```

### Materalized view
We always encounter a situation that we need to execute heavy queries frequently. In this case, we will have a bad user experience and increase compute consumption. To remediate this incident, we can use a materialized view table. 

```http
CREATE OR REPLACE MATERIALIZED VIEW ORDERS_MV
AS 
SELECT
YEAR(O_ORDERDATE) AS YEAR,
MAX(O_COMMENT) AS MAX_COMMENT,
MIN(O_COMMENT) AS MIN_COMMENT,
MAX(O_CLERK) AS MAX_CLERK,
MIN(O_CLERK) AS MIN_CLERK
FROM ORDERS.TPCH_SF100.ORDERS
GROUP BY YEAR(O_ORDERDATE);
```


### Data Masking 
As we are dealing with customer's data, we always encounter confidential information for our customers such as their email address, phone number, and even their SSN. It is a best practice to mask the confidential information so that only appropriate role can view the confidential. We can do these with data masking function. 

```http
-- Set up masking policy

create or replace masking policy phone 
    as (val varchar) returns varchar ->
            case        
            when current_role() in ('ANALYST_FULL', 'ACCOUNTADMIN') then val
            else '##-###-##'
            end;
  

-- Apply policy on a specific column 
ALTER TABLE IF EXISTS CUSTOMERS MODIFY COLUMN phone 
SET MASKING POLICY PHONE;
```
