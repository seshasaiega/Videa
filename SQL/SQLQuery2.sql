USE [ProviderIntake]
GO
/****** Object:  StoredProcedure [AD].[GetProviderForReferral]    Script Date: 01/14/2016 16:53:22 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

      
/*                                                                                      
  Procedure Name:[AD].[GetProviderForReferral]                                                            
  Purpose:  Get complete details a member referral detail based on its ReferralID                                                               
  Author:   Prasan Johnson        
  Modified by : Sreenath Padmanabhan      
  Modified date: 12/17/2014                                                                                
  Date Created: March 2014                                                                                    
*/      
--[AD].[GETPROVIDERFORREFERRAL] 148
ALTER PROCEDURE [AD].[GetProviderForReferral] --21    
(      
 @REFERRALID INT,      
 @SERVICEID INT = NULL            
)              
AS              
BEGIN              
              
	DECLARE @REFERRALTYPEDESC VARCHAR(100)              
	DECLARE @RECENTPROVID INT              
	DECLARE @PREVRECENTPROVID INT              
	DECLARE @PROVID INT              
	DECLARE @ORDERINDEX INT          
	DECLARE @MEMDOB DATETIME    
	DECLARE @YEARS int
	DECLARE @MONTHS int
	DECLARE @TYPEOFPROVIDER VARCHAR(7)
	DECLARE @MEMZIP VARCHAR(15)

	SELECT @MEMDOB = MEMDOB FROM 
	AD.MEMBERREFERRAL WHERE REFERRALID =  @REFERRALID	
	SELECT  @MEMZIP=MEMZIP FROM 
	AD.MEMBERREFERRAL WHERE REFERRALID =  @REFERRALID	
		
	SELECT @MEMDOB = CONVERT(datetime, @MEMDOB, 101)

	print @MEMDOB
	print @MEMZIP
	
	SELECT @YEARS=DATEDIFF(MONTH,CASE WHEN DAY(@MEMDOB) > DAY(getdate())THEN DATEADD(MONTH,1,@MEMDOB)ELSE @MEMDOB END,getdate()) / 12 
	SELECT @MONTHS=DATEDIFF(MONTH,CASE WHEN DAY(@MEMDOB) > DAY(getdate())THEN DATEADD(MONTH,1,@MEMDOB)ELSE @MEMDOB END,getdate()) % 12 
	
	Declare @LocalZIPDiatances TABLE(
	ZIP varchar(20),
	DISTANCE float)
	
	Declare @ProviderServices Table(
	PROVID INT,
	SERVICEFLAG INT)
	
	INSERT INTO @ProviderServices
	SELECT PROVIDERID,0 FROM AD.PROVIDER
	
	UPDATE @ProviderServices SET SERVICEFLAG=1 WHERE PROVID IN (SELECT PROVIDERID FROM AD.PROVIDERSERVICE WHERE SERVICEID=@SERVICEID)
	
		
	--CREATE TABLE #LocalZIPDiatances
	--ZIP varchar(20),
	--DISTANCE float))

	Insert into @LocalZIPDiatances
	SELECT     PR.ZIP,sqrt(
	          (WPS.latitude-WM.latitude)*(WPS.latitude-WM.latitude)+
	          (WPS.longitude-WM.longitude)*(WPS.longitude-WM.longitude))*69.1
	FROM       [WebProviderSearch].[dbo].[Zip] WM,  AD.PROVIDER PR,[WebProviderSearch].[dbo].[Zip] WPS
	
	where WM.ZIP=@MEMZIP and PR.ZIP= WPS.ZIP
       
       
	IF ((@YEARS = 17 AND @MONTHS < 6) OR (@YEARS < 17))
	SELECT @TYPEOFPROVIDER='CHILD'
	ELSE IF (@YEARS = 17 AND @MONTHS > 6)
	SELECT @TYPEOFPROVIDER='BOTH'	
	ELSE if(@YEARS >= 18)
	SELECT @TYPEOFPROVIDER='ADULT'
              
	 SELECT @REFERRALTYPEDESC = REFERRALTYPEDESC FROM AD.REFERRALTYPE RT              
	 JOIN AD.MEMBERREFERRAL MR              
	 ON MR.REFERRALTYPEID = RT.REFERRALTYPEID              
	 AND MR.REFERRALID = @REFERRALID   
   
   
    SELECT @MEMDOB = @MEMDOB-14

	IF (@REFERRALTYPEDESC = 'Child Urgent')              
	              
	 BEGIN             
	    
	    
	    SELECT  DISTINCT   AD.PROVIDER.PROVIDERID, AD.PROVIDER.NAME, AD.PROVIDER.ADDRESS1, AD.PROVIDER.ADDRESS2, AD.PROVIDER.CITY, AD.PROVIDER.STATE, 
                      AD.PROVIDER.ZIP, AD.PROVIDER.PHONE, AD.PROVIDEREFERRALTYPE.Capacity AS AVAILABLECAPACITY, AD.PROVIDEREFERRALTYPE.HOURSOFOPERATION, GETDATE() LASTASSIGNDATE, 
                      (select [AD].ProviderTotalRef(AD.PROVIDER.PROVIDERID,4)) as TotalReferrals, LD.DISTANCE, PS.SERVICEFLAG,AD.PROVIDEREFERRALTYPE.Capacity-(select [AD].ProviderTotalRef(AD.PROVIDER.PROVIDERID,4))as LefCap
				FROM         AD.PROVIDER INNER JOIN
                      AD.PROVIDEREFERRALTYPE ON AD.PROVIDER.PROVIDERID = AD.PROVIDEREFERRALTYPE.PROVIDERID 
                      Left JOIN @LocalZIPDiatances LD ON AD.PROVIDER.ZIP= LD.ZIP
                      LEFT JOIN @ProviderServices PS ON AD.PROVIDER.PROVIDERID=PS.PROVID
                Where AD.PROVIDEREFERRALTYPE.REFERRALTYPEID=4              	 	    
	                
	END        
	
	
	ELSE IF (@REFERRALTYPEDESC = 'SMI Assessment')       
	BEGIN      
		SELECT    DISTINCT AD.PROVIDER.PROVIDERID, AD.PROVIDER.NAME, AD.PROVIDER.ADDRESS1, AD.PROVIDER.ADDRESS2, AD.PROVIDER.CITY, AD.PROVIDER.STATE, 
                      AD.PROVIDER.ZIP, AD.PROVIDER.PHONE, AD.PROVIDEREFERRALTYPE.Capacity AS AVAILABLECAPACITY,GETDATE() LASTASSIGNDATE, AD.PROVIDEREFERRALTYPE.HOURSOFOPERATION, 
                      (select [AD].ProviderTotalRef(AD.PROVIDER.PROVIDERID,6)) as TotalReferrals, LD.DISTANCE, PS.SERVICEFLAG,AD.PROVIDEREFERRALTYPE.Capacity-(select [AD].ProviderTotalRef(AD.PROVIDER.PROVIDERID,5))as LefCap
				FROM         AD.PROVIDER INNER JOIN
                      AD.PROVIDEREFERRALTYPE ON AD.PROVIDER.PROVIDERID = AD.PROVIDEREFERRALTYPE.PROVIDERID 
                      left JOIN @LocalZIPDiatances LD ON AD.PROVIDER.ZIP= LD.ZIP
                      LEFT JOIN @ProviderServices PS ON AD.PROVIDER.PROVIDERID=PS.PROVID
                Where AD.PROVIDEREFERRALTYPE.REFERRALTYPEID = 6
	    
	END
	
	ELSE IF (@REFERRALTYPEDESC = 'Adult Urgent')              
	              
	 BEGIN        
	    SELECT   DISTINCT  AD.PROVIDER.PROVIDERID, AD.PROVIDER.NAME, AD.PROVIDER.ADDRESS1, AD.PROVIDER.ADDRESS2, AD.PROVIDER.CITY, AD.PROVIDER.STATE, 
                      AD.PROVIDER.ZIP, AD.PROVIDER.PHONE, AD.PROVIDEREFERRALTYPE.Capacity AS AVAILABLECAPACITY,GETDATE() LASTASSIGNDATE, AD.PROVIDEREFERRALTYPE.HOURSOFOPERATION, 
                      (select [AD].ProviderTotalRef(AD.PROVIDER.PROVIDERID,2)) as TotalReferrals, LD.DISTANCE, PS.SERVICEFLAG,AD.PROVIDEREFERRALTYPE.Capacity-(select [AD].ProviderTotalRef(AD.PROVIDER.PROVIDERID,5))as LefCap
				FROM         AD.PROVIDER INNER JOIN
                      AD.PROVIDEREFERRALTYPE ON AD.PROVIDER.PROVIDERID = AD.PROVIDEREFERRALTYPE.PROVIDERID 
                      Left JOIN @LocalZIPDiatances LD ON AD.PROVIDER.ZIP= LD.ZIP
                      LEFT JOIN @ProviderServices PS ON AD.PROVIDER.PROVIDERID=PS.PROVID
                Where AD.PROVIDEREFERRALTYPE.REFERRALTYPEID=2          	 	    
	                
	END        
	
	ELSE IF (@REFERRALTYPEDESC = 'Immediate/Crisis')              
	              
	 BEGIN        
	    SELECT   DISTINCT  AD.PROVIDER.PROVIDERID, AD.PROVIDER.NAME, AD.PROVIDER.ADDRESS1, AD.PROVIDER.ADDRESS2,GETDATE() LASTASSIGNDATE, AD.PROVIDER.CITY, AD.PROVIDER.STATE, 
                      AD.PROVIDER.ZIP, AD.PROVIDER.PHONE, AD.PROVIDEREFERRALTYPE.Capacity AS AVAILABLECAPACITY, AD.PROVIDEREFERRALTYPE.HOURSOFOPERATION, 
                      (select [AD].ProviderTotalRef(AD.PROVIDER.PROVIDERID,1)) as TotalReferrals, LD.DISTANCE, PS.SERVICEFLAG,AD.PROVIDEREFERRALTYPE.Capacity-(select [AD].ProviderTotalRef(AD.PROVIDER.PROVIDERID,5))as LefCap
				FROM         AD.PROVIDER INNER JOIN
                      AD.PROVIDEREFERRALTYPE ON AD.PROVIDER.PROVIDERID = AD.PROVIDEREFERRALTYPE.PROVIDERID 
                      Left JOIN @LocalZIPDiatances LD ON AD.PROVIDER.ZIP= LD.ZIP
                      LEFT JOIN @ProviderServices PS ON AD.PROVIDER.PROVIDERID=PS.PROVID
                Where AD.PROVIDEREFERRALTYPE.REFERRALTYPEID=1            	 	    
	                
	END 
	
	ELSE IF (@REFERRALTYPEDESC = 'SAPT')              
	              
	 BEGIN   
	 
	    SELECT   DISTINCT  AD.PROVIDER.PROVIDERID, AD.PROVIDER.NAME, AD.PROVIDER.ADDRESS1, AD.PROVIDER.ADDRESS2, AD.PROVIDER.CITY,GETDATE() LASTASSIGNDATE, AD.PROVIDER.STATE, 
                      AD.PROVIDER.ZIP, AD.PROVIDER.PHONE, AD.PROVIDEREFERRALTYPE.Capacity AS AVAILABLECAPACITY, AD.PROVIDEREFERRALTYPE.HOURSOFOPERATION, 
                      (select [AD].ProviderTotalRef(AD.PROVIDER.PROVIDERID,11)) as TotalReferrals, LD.DISTANCE,PS.SERVICEFLAG,AD.PROVIDEREFERRALTYPE.Capacity-(select [AD].ProviderTotalRef(AD.PROVIDER.PROVIDERID,5))as LefCap
				FROM         AD.PROVIDER INNER JOIN
                      AD.PROVIDEREFERRALTYPE ON AD.PROVIDER.PROVIDERID = AD.PROVIDEREFERRALTYPE.PROVIDERID 
                      Left JOIN @LocalZIPDiatances LD ON AD.PROVIDER.ZIP= LD.ZIP
                      LEFT JOIN @ProviderServices PS ON AD.PROVIDER.PROVIDERID=PS.PROVID
                Where AD.PROVIDEREFERRALTYPE.REFERRALTYPEID=11   order by  TotalReferrals      	 	    
	                
	END  
	
	      
	ELSE IF (@REFERRALTYPEDESC = 'Child Routine' OR @REFERRALTYPEDESC = 'Adult Routine' OR @REFERRALTYPEDESC = 'Inter-RBHA Transfer' OR @REFERRALTYPEDESC = '1 time Consultation' OR @REFERRALTYPEDESC = 'Psych Referral' OR @REFERRALTYPEDESC = 'PCP/Health Plan')              
	              
	 BEGIN    
	 
	 IF (@TYPEOFPROVIDER='CHILD')           
		    SELECT    DISTINCT AD.PROVIDER.PROVIDERID, AD.PROVIDER.NAME, AD.PROVIDER.ADDRESS1, AD.PROVIDER.ADDRESS2, AD.PROVIDER.CITY, AD.PROVIDER.STATE, GETDATE() LASTASSIGNDATE,
                      AD.PROVIDER.ZIP, AD.PROVIDER.PHONE, AD.PROVIDEREFERRALTYPE.Capacity AS AVAILABLECAPACITY, AD.PROVIDEREFERRALTYPE.HOURSOFOPERATION, 
                      (select [AD].ProviderTotalRef(AD.PROVIDER.PROVIDERID,5)) as TotalReferrals, LD.DISTANCE,PS.SERVICEFLAG,AD.PROVIDEREFERRALTYPE.Capacity-(select [AD].ProviderTotalRef(AD.PROVIDER.PROVIDERID,5))as LefCap
				FROM         AD.PROVIDER INNER JOIN
                      AD.PROVIDEREFERRALTYPE ON AD.PROVIDER.PROVIDERID = AD.PROVIDEREFERRALTYPE.PROVIDERID 
                      LEFT JOIN @LocalZIPDiatances LD ON AD.PROVIDER.ZIP= LD.ZIP            
                      LEFT JOIN @ProviderServices PS ON AD.PROVIDER.PROVIDERID=PS.PROVID          
                Where AD.PROVIDEREFERRALTYPE.REFERRALTYPEID = 5	       
	          
	 ELSE IF (@TYPEOFPROVIDER='ADULT')           
		    SELECT  DISTINCT   AD.PROVIDER.PROVIDERID, AD.PROVIDER.NAME, AD.PROVIDER.ADDRESS1, AD.PROVIDER.ADDRESS2, AD.PROVIDER.CITY, AD.PROVIDER.STATE, GETDATE() LASTASSIGNDATE,
                      AD.PROVIDER.ZIP, AD.PROVIDER.PHONE, AD.PROVIDEREFERRALTYPE.Capacity AS AVAILABLECAPACITY, AD.PROVIDEREFERRALTYPE.HOURSOFOPERATION, 
                      (select [AD].ProviderTotalRef(AD.PROVIDER.PROVIDERID,3)) as TotalReferrals, LD.DISTANCE, PS.SERVICEFLAG,AD.PROVIDEREFERRALTYPE.Capacity-(select [AD].ProviderTotalRef(AD.PROVIDER.PROVIDERID,5))as LefCap
				FROM         AD.PROVIDER INNER JOIN
                      AD.PROVIDEREFERRALTYPE ON AD.PROVIDER.PROVIDERID = AD.PROVIDEREFERRALTYPE.PROVIDERID 
                      LEFT JOIN @LocalZIPDiatances LD ON AD.PROVIDER.ZIP= LD.ZIP
                      LEFT JOIN @ProviderServices PS ON AD.PROVIDER.PROVIDERID=PS.PROVID
                Where AD.PROVIDEREFERRALTYPE.REFERRALTYPEID = 3 
                
      ELSE IF (@TYPEOFPROVIDER='BOTH')           
		    
	        SELECT    DISTINCT PROVIDER.PROVIDERID, PROVIDER.NAME, PROVIDER.ADDRESS1, PROVIDER.ADDRESS2, PROVIDER.CITY, PROVIDER.[STATE], GETDATE() LASTASSIGNDATE,
                      PROVIDER.ZIP, PROVIDER.PHONE,PROVIDEREFERRALTYPE.Capacity AS AVAILABLECAPACITY, PROVIDEREFERRALTYPE.HOURSOFOPERATION, 
                      (select [AD].ProviderTotalRef(AD.PROVIDER.PROVIDERID,3)) as TotalReferrals, LD.DISTANCE, PS.SERVICEFLAG,AD.PROVIDEREFERRALTYPE.Capacity-(select [AD].ProviderTotalRef(AD.PROVIDER.PROVIDERID,5))as LefCap
				FROM         AD.PROVIDER INNER JOIN
                      AD.PROVIDEREFERRALTYPE ON AD.PROVIDER.PROVIDERID = AD.PROVIDEREFERRALTYPE.PROVIDERID 
                      LEFT JOIN @LocalZIPDiatances LD ON AD.PROVIDER.ZIP= LD.ZIP
                      LEFT JOIN @ProviderServices PS ON AD.PROVIDER.PROVIDERID=PS.PROVID
                Where AD.PROVIDEREFERRALTYPE.REFERRALTYPEID = 3  
                
                UNION
                
             SELECT  DISTINCT   AD.PROVIDER.PROVIDERID, AD.PROVIDER.NAME, AD.PROVIDER.ADDRESS1, AD.PROVIDER.ADDRESS2, AD.PROVIDER.CITY, AD.PROVIDER.STATE, GETDATE() LASTASSIGNDATE,
                      AD.PROVIDER.ZIP, AD.PROVIDER.PHONE, AD.PROVIDEREFERRALTYPE.Capacity AS AVAILABLECAPACITY, AD.PROVIDEREFERRALTYPE.HOURSOFOPERATION, 
                      (select [AD].ProviderTotalRef(AD.PROVIDER.PROVIDERID,5)) as TotalReferrals, LD.DISTANCE, PS.SERVICEFLAG,AD.PROVIDEREFERRALTYPE.Capacity-(select [AD].ProviderTotalRef(AD.PROVIDER.PROVIDERID,5))as LefCap
				FROM         AD.PROVIDER INNER JOIN
                      AD.PROVIDEREFERRALTYPE ON AD.PROVIDER.PROVIDERID = AD.PROVIDEREFERRALTYPE.PROVIDERID 
                      LEFT JOIN @LocalZIPDiatances LD ON AD.PROVIDER.ZIP= LD.ZIP
                      LEFT JOIN @ProviderServices PS ON AD.PROVIDER.PROVIDERID=PS.PROVID
                Where AD.PROVIDEREFERRALTYPE.REFERRALTYPEID = 5 
                          
	END              

	ELSE               
		 SELECT    DISTINCT PROVIDER.PROVIDERID, PROVIDER.NAME, PROVIDER.ADDRESS1, PROVIDER.ADDRESS2, PROVIDER.CITY, PROVIDER.[STATE], GETDATE() LASTASSIGNDATE,
                      PROVIDER.ZIP, PROVIDER.PHONE,PROVIDEREFERRALTYPE.Capacity AS AVAILABLECAPACITY, PROVIDEREFERRALTYPE.HOURSOFOPERATION, 
                      (select [AD].ProviderTotalRef(AD.PROVIDER.PROVIDERID,3)) as TotalReferrals, LD.DISTANCE,PS.SERVICEFLAG,AD.PROVIDEREFERRALTYPE.Capacity-(select [AD].ProviderTotalRef(AD.PROVIDER.PROVIDERID,5))as LefCap
				FROM         AD.PROVIDER INNER JOIN
                      AD.PROVIDEREFERRALTYPE ON AD.PROVIDER.PROVIDERID = AD.PROVIDEREFERRALTYPE.PROVIDERID 
                      Left JOIN @LocalZIPDiatances LD ON AD.PROVIDER.ZIP= LD.ZIP
                      LEFT JOIN @ProviderServices PS ON AD.PROVIDER.PROVIDERID=PS.PROVID
                Where AD.PROVIDEREFERRALTYPE.REFERRALTYPEID = 3   
                
                UNION
                
             SELECT   DISTINCT  PROVIDER.PROVIDERID, PROVIDER.NAME, PROVIDER.ADDRESS1, PROVIDER.ADDRESS2, PROVIDER.CITY, PROVIDER.STATE, GETDATE() LASTASSIGNDATE,
                      PROVIDER.ZIP, PROVIDER.PHONE, PROVIDEREFERRALTYPE.Capacity AS AVAILABLECAPACITY, PROVIDEREFERRALTYPE.HOURSOFOPERATION, 
                      (select AD.ProviderTotalRef(PROVIDER.PROVIDERID,5)) as TotalReferrals, LD.DISTANCE,PS.SERVICEFLAG,AD.PROVIDEREFERRALTYPE.Capacity-(select [AD].ProviderTotalRef(AD.PROVIDER.PROVIDERID,5))as LefCap
				FROM         AD.PROVIDER INNER JOIN
                      AD.PROVIDEREFERRALTYPE ON AD.PROVIDER.PROVIDERID = AD.PROVIDEREFERRALTYPE.PROVIDERID 
                      Left JOIN @LocalZIPDiatances LD ON AD.PROVIDER.ZIP= LD.ZIP 
                      LEFT JOIN @ProviderServices PS ON AD.PROVIDER.PROVIDERID=PS.PROVID
                Where AD.PROVIDEREFERRALTYPE.REFERRALTYPEID = 5 
		        
                
END           
      
