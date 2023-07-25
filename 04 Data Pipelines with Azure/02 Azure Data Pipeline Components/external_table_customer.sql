-- Use the same file format as used for creating the External Tables during the LOAD step.
IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'SynapseDelimitedTextFormat') 
    CREATE EXTERNAL FILE FORMAT [SynapseDelimitedTextFormat] 
    WITH ( FORMAT_TYPE = DELIMITEDTEXT ,
           FORMAT_OPTIONS (
             FIELD_TERMINATOR = ',',
             USE_TYPE_DEFAULT = FALSE
            ))
GO;

-- Storage path where the result set will persist
IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'customer_dfs_core_windows_net') 
    CREATE EXTERNAL DATA SOURCE [customer_dfs_core_windows_net] 
    WITH (
        LOCATION = 'https://udasynapsedlg2acc.blob.core.windows.net/udasynapsedlg2files/external_table_data' 
    )
GO;


CREATE EXTERNAL TABLE [dbo].[Customer](
    [CustomerID] [int],
    [NameStyle]  [bit],
    [Title] [nvarchar](8) NULL,
    [FirstName] [nvarchar](128),
    [MiddleName] [nvarchar](20) NULL,
    [LastName] [nvarchar](128),
    [Suffix] [nvarchar](10) NULL,
    [CompanyName] [nvarchar](128) NULL,
    [SalesPerson] [nvarchar](256) NULL,
    [EmailAddress] [nvarchar](50) NULL,
    [Phone] [nvarchar](20) NULL,
    [PasswordHash] [varchar](128),
    [PasswordSalt] [varchar](10),
    [rowguid] [uniqueidentifier],
    [ModifiedDate] [datetime]
)
WITH (
		LOCATION = 'customer.csv',
      DATA_SOURCE = [customer_dfs_core_windows_net],
      FILE_FORMAT = [SynapseDelimitedTextFormat]
)
GO;