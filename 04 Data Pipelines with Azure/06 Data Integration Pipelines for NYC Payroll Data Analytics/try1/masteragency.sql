USE udapipelineproj
GO;

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
IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'mydlsfs20230712_mydls20230712_dfs_core_windows_net') 
    CREATE EXTERNAL DATA SOURCE [mydlsfs20230712_mydls20230712_dfs_core_windows_net] 
    WITH (
        LOCATION = 'https://udaprojsynapsedlg2acc.blob.core.windows.net/udaprojsynapsedlg2filesys' 
    )
GO;

CREATE EXTERNAL TABLE [dbo].[NYC_Payroll_AGENCY_MD](
    [AgencyID] [varchar](50) NULL,
    [AgencyName] [varchar](150) NULL
)
WITH (
		LOCATION = 'payroll_agency_md.csv',
      DATA_SOURCE = [mydlsfs20230712_mydls20230712_dfs_core_windows_net],
      FILE_FORMAT = [SynapseDelimitedTextFormat]
)
GO;
