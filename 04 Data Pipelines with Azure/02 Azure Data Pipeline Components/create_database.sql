/*

CREATE statement isn't supported in the master database

If your query fails with the error message Failed to execute query. 
    Error: CREATE EXTERNAL TABLE/DATA SOURCE/DATABASE SCOPED CREDENTIAL/FILE FORMAT is not
             supported in master database.,it means that the master database in 
            serverless SQL pool doesn't support the creation of:

https://learn.microsoft.com/en-us/azure/synapse-analytics/sql/resources-self-help-sql-on-demand?tabs=x80070002#create-statement-is-not-supported-in-master-database

*/


CREATE DATABASE uda_demo;