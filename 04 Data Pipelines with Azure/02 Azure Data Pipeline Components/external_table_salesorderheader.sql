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
IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'sales_orderheader_dfs_core_windows_net') 
    CREATE EXTERNAL DATA SOURCE [sales_orderheader_dfs_core_windows_net] 
    WITH (
        LOCATION = 'https://udasynapsedlg2acc.blob.core.windows.net/udasynapsedlg2files/external_table_data' 
    )
GO;

CREATE EXTERNAL TABLE [dbo].[SalesOrderHeader](
      [SalesOrderID] [int] ,
      [RevisionNumber] [tinyint] ,
      [OrderDate] [datetime] ,
      [DueDate] [datetime] ,
      [ShipDate] [datetime] NULL,
      [Status] [tinyint] ,
      [OnlineOrderFlag] [BIT] ,
      [SalesOrderNumber]  [nvarchar](23) ,
      [PurchaseOrderNumber] [nvarchar](23) NULL,
      [AccountNumber] [nvarchar](23) NULL,
      [CustomerID] [int] ,
      [ShipToAddressID] [int] NULL,
      [BillToAddressID] [int] NULL,
      [ShipMethod] [nvarchar](50) ,
      [CreditCardApprovalCode] [varchar](15) NULL,
      [SubTotal] [money] ,
      [TaxAmt] [money] ,
      [Freight] [money] ,
      [TotalDue]  [money] NULL,
      [Comment] [nvarchar](1000) NULL,
      [rowguid] [uniqueidentifier] ,
      [ModifiedDate] [datetime] 
)
WITH (
		LOCATION = 'SalesOrderHeader.csv',
      DATA_SOURCE = [sales_orderheader_dfs_core_windows_net],
      FILE_FORMAT = [SynapseDelimitedTextFormat]
)
GO;