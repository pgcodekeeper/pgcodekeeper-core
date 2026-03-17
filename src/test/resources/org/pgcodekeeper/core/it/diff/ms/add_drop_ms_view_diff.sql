DROP VIEW IF EXISTS [dbo].[view1]
GO

SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO
CREATE VIEW [dbo].[view1] AS
    SELECT 
    a.[c1],
    a.[c2]
FROM [dbo].[table1] a
GO

DROP VIEW IF EXISTS [dbo].[view2]
GO

SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO
CREATE VIEW [dbo].[view2] AS
    SELECT 
    a.[c1],
    a.[c2]
FROM [dbo].[table1] a
GO

DROP VIEW IF EXISTS [dbo].[view3]
GO

SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO
CREATE VIEW [dbo].[view3] AS
    SELECT 
    a.[c1],
    a.[c2]
FROM [dbo].[table1] a
GO