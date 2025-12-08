CREATE TABLE [dbo].[t6](
    [col1] [int] NULL,
    [col2] [int] NULL,
    [col3] [int] NULL
) ON [PRIMARY]
GO

CREATE STATISTICS stat1 ON [dbo].[t6] (col1, col2);

CREATE STATISTICS stat2 ON [dbo].[t6] (col1, col2, col3)
WITH FULLSCAN;

CREATE STATISTICS stat3 ON [dbo].[t6] (col1, col2, col3)
WITH SAMPLE 16 PERCENT, PERSIST_SAMPLE_PERCENT = ON;

CREATE TABLE [dbo].[t4](
    [col1] [int] NULL,
    [col2] [int] NULL,
    [col3] [int] NULL
) ON [PRIMARY]
GO

CREATE STATISTICS stat1 ON [dbo].[t4] (col1, col2)
WHERE ([col2]=(2))
WITH NORECOMPUTE;

CREATE STATISTICS stat2 ON [dbo].[t4] (col1, col2)
WHERE ([col2]=(2)) WITH AUTO_DROP = ON;

CREATE PARTITION FUNCTION myRangePF1 (datetime2(0))
    AS RANGE RIGHT FOR VALUES ('2022-04-01', '2022-05-01', '2022-06-01') ;
GO

CREATE PARTITION SCHEME myRangePS1
    AS PARTITION myRangePF1
    ALL TO ('PRIMARY') ;
GO

CREATE TABLE dbo.PartitionTable (
    [col1] [datetime2] (0) NOT NULL,
    [col2] [char] (10) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL)
    ON myRangePS1([col1]) ;
GO

ALTER TABLE [dbo].[PartitionTable]
	ADD CONSTRAINT [PK__Partitio__357D0D3E9BAF9BB1] PRIMARY KEY CLUSTERED ([col1], [col2]) ON [myRangePS1]([col1])
GO

CREATE TABLE dbo.PartitionTable2 (
    [col1] [datetime2] (0) CONSTRAINT [PK__Partitio__357D0D3E42EB1890] PRIMARY KEY CLUSTERED  ([col1]) ON [myRangePS1]([col1]) NOT NULL ,
    [col2] [char] (10) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL)
    ON myRangePS1([col1]) ;
GO

CREATE STATISTICS stat1 ON [dbo].[PartitionTable] (col1, col2)
WITH INCREMENTAL = ON
GO