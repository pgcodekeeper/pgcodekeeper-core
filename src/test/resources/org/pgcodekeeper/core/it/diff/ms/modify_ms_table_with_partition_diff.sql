DROP TABLE [dbo].[table_partition_01]
GO

ALTER TABLE [dbo].[table_partition_02]
	ADD CONSTRAINT [part_name] PRIMARY KEY CLUSTERED  ([id]) ON [ps_range_left]([id])
GO

SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO
CREATE TABLE [dbo].[table_partition_01](
	[id] [int] NULL
) ON [ps_range_right]([id])
GO