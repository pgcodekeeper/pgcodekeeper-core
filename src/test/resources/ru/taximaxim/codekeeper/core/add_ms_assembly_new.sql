-- Assume that assembly binary is correct

CREATE ASSEMBLY [Hi]
AUTHORIZATION [dbo]
FROM 0x4D5A900003 
WITH PERMISSION_SET = SAFE
GO
ALTER ASSEMBLY [Hi] WITH VISIBILITY = OFF
GO