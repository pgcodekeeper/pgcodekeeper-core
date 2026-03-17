CREATE FUNCTION [dbo].[ReturnOperResult](@First int, @Second int) 
RETURNS integer
AS
BEGIN
  DECLARE @Res integer = 0;
  
  SET @Res = @First - @Second + 1;

  IF @Res < 0
    SET @Res = 0;
  
  RETURN @Res;
END
GO
------------------------------------------------------------
CREATE FUNCTION [dbo].[cats](@First int, @Second int) 
RETURNS integer
AS
BEGIN
  DECLARE @Res integer = 0;
  
  SET @Res = @First - @Second + 1;

  IF @Res < 0
    SET @Res = 0;
  
  RETURN @Res;
END
GO
--------------------------------------------------------------
CREATE FUNCTION [dbo].[dogs](@First int, @Second int) 
RETURNS integer
AS
BEGIN
  DECLARE @Res integer = 0;
  
  SET @Res = @First - @Second + 1;

  IF @Res < 0
    SET @Res = 0;
  
  RETURN @Res;
END
GO