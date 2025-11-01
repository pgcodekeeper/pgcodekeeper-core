SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO
CREATE TABLE [dbo].[table1](
    [c1] [int] NOT NULL,
    [c2] [int] NOT NULL,
    [c3] [varchar] (100) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL
) ON [PRIMARY]
GO

CREATE CLUSTERED INDEX [index_c2] ON [dbo].[table1] ([c2]) ON [PRIMARY]
GO

CREATE TABLE [dbo].[table2](
    [c1] [int] NOT NULL,
    [c2] [int] NOT NULL,
    [c3] [varchar] (100) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL
) ON [PRIMARY]
GO

CREATE UNIQUE INDEX [idx_1] ON [dbo].[table2] ([c3]) WITH (IGNORE_DUP_KEY = ON) ON [PRIMARY];
GO

-- test nonclusterd columnstore index
CREATE TABLE [dbo].[table3](
	[col1] [int] NULL,
	[col2] [int] NULL,
	[col3] [int] NULL
) ON [PRIMARY]
GO

CREATE NONCLUSTERED COLUMNSTORE INDEX [idx_table3] ON [dbo].[table3] ([col1])
WITH (DATA_COMPRESSION = COLUMNSTORE_ARCHIVE)
ON [PRIMARY]
GO

-- add table with CLUSTERED COLUMNSTORE ORDER index
CREATE TABLE dbo.t4
(
    [c1] [int] NOT NULL,
    [c2] [int] NOT NULL,
    [c3] [varchar] (100) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    INDEX [ix_4] CLUSTERED COLUMNSTORE ORDER ([c2], [c1])
) ON [PRIMARY]
GO

CREATE TABLE dbo.t5
(
    [c1] [int] NOT NULL,
    [c2] [int] NOT NULL,
    [c3] [varchar] (100) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL
) ON [PRIMARY]
GO

CREATE NONCLUSTERED INDEX [noncl_idx] ON [dbo].[t5]([c1])
WITH (FILLFACTOR = 80, PAD_INDEX = ON)
ON [PRIMARY]
GO

CREATE VIEW dbo.v1
WITH SCHEMABINDING
AS SELECT
     [c1],
     [c2]
FROM dbo.t5
GO

CREATE UNIQUE CLUSTERED INDEX v_idx ON dbo.v1 ([c2]) ON [PRIMARY]
GO
