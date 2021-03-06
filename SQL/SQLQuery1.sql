USE [ProviderIntake]
GO
/****** Object:  StoredProcedure [AD].[uspAdultProvMonthlyReport]    Script Date: 01/14/2016 16:52:20 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [AD].[uspAdultProvMonthlyReport]

/*
		Object			: AD.[uspAdultProvMonthlyReport] 
		Description		: To pull Report for Adult Provider Monthly Report
		Usage			: Used to retrieve Ault Provider Monthly Report
		Created			: 07.21.2015
		Created By		: Sesha Sai

		Modification history
		Mod Date	User		Description
		--- ----	----		-----------	

*/ 

	@Year	varchar(23)
	
AS
BEGIN

select P.NAME, P.ADDRESS1,P.CITY,P.ZIP,DATENAME(month,MR.UpdateDate) as date,COUNT(*) as cou 
into #tmp
from ad.MEMBERREFERRAL MR inner join 

ad.PROVIDER P on MR.PROVIDERID=P.PROVIDERID inner join 

Ad.PROVIDEREFERRALTYPE PRT on P.PROVIDERID=PRT.PROVIDERID

and MR.UpdateDate Between DATEADD(yy, DATEDIFF(yy, 0, @Year), 0) and DATEADD(yy, DATEDIFF(yy, 0, @Year)+1 , 0)
where PRT.REFERRALTYPEID=3

group by P.NAME, P.ADDRESS1,P.CITY,P.ZIP,DATENAME(month,MR.UpdateDate)

Alter table #tmp
add YTD Int;

select NAME, ADDRESS1,CITY,ZIP,SUM(cou) as YTD into #tmp2 from #tmp group by NAME, ADDRESS1,CITY,ZIP 

Update  #tmp set #tmp.YTD=(select #tmp2.YTD from #tmp2 where #tmp2.NAME=#tmp.NAME and #tmp2.CITY=#tmp.CITY and #tmp.ZIP=#tmp2.ZIP and #tmp.ADDRESS1=#tmp2.ADDRESS1)

DECLARE @columns AS VARCHAR(MAX);
DECLARE @columnsSelect AS VARCHAR(MAX);
DECLARE @MTD AS VARCHAR(MAX);
DECLARE @sql AS VARCHAR(MAX);

select @columns = substring((Select DISTINCT ',' + ISNULL(QUOTENAME(date),0) FROM #tmp FOR XML PATH ('')),2, 1000);
select @columnsSelect =substring((Select DISTINCT ',ISNULL('+QUOTENAME(date)+',0) as'+ ISNULL(QUOTENAME(date),0)  FROM #tmp FOR XML PATH ('')),2, 1000)
select @MTD =substring((Select DISTINCT ',CONVERT(INT,ISNULL('+QUOTENAME(date)+',0))' FROM #tmp FOR XML PATH ('')),2, 1000)
--select @MTD =substring((Select DISTINCT ',CONVERT(INT,'+QUOTENAME(date)+')' FROM #tmp FOR XML PATH ('')),2, 1000)

Print @columns
Print @columnsSelect
Print @MTD

SELECT @sql =

'SELECT NAME, ADDRESS1,CITY,ZIP,YTD,'+@columnsSelect+'
FROM #tmp
PIVOT 
(
  Max(cou)
  FOR date IN( ' + @columns + ' )) as counts;';

execute(@sql);


END
