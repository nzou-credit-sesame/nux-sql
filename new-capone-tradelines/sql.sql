
select  date_trunc('month', acct_registration_complete_datetime) as registration_month
	, count(distinct usertoken)
from (
		select a.usertoken
		, idhash
		, acct_registration_complete_datetime
		from 
			(		
			select usertoken
			, idhash
			, dateopened
			from credit_report_tradelines
			where creditor ilike '%CAP%ONE%'
	    	OR creditor ilike '%CAP1%'
	    	and creditor not ilike '%capstone%'
	    	and dateopened >= '2016-01-01'
	    	group by 1,2,3
    		) a
		join public.user b
		on a.usertoken =b.usertoken
		and dateopened <= acct_registration_complete_datetime + 30
		where 1
		and dateopened >= acct_registration_complete_datetime
		and acct_registration_complete_datetime >= '2016-01-01'
	)
group by 1
order by 1







select registration_day
, count(distinct clickid)
from (

select cap.usertoken
, date_trunc('month', acct_registration_complete_datetime) as registration_day
, clickid
from click_applies_properties cap
join

	(select distinct usertoken, acct_registration_complete_datetime
	from public.user
	join credit_profile_history using (usertoken)
	where acct_registration_complete_datetime >= '2016-01-01'
	and datediff('day', acct_registration_complete_datetime, reportpulldate) >= 30
	)
using (usertoken)
join revenue.all_partners_revenue_data rev 
using (clickid)
where 1
and coalesce(cap.createdate, rev.timeclicked) > acct_registration_complete_datetime
and datediff('day', acct_registration_complete_datetime, coalesce(cap.createdate, rev.timeclicked)) <= 30
-- two things!!!!
and partner ilike 'capital%one%'
and coalesce(cap.createdate, rev.timeclicked) > acct_registration_complete_datetime
)
group by 1
order by 1






select count(distinct usertoken)
from public.user
  join credit_profile_history using (usertoken)
where date_trunc('month', acct_registration_complete_datetime) = '2017-09-01'
      or date_trunc('month', acct_registration_complete_datetime) = '2017-10-01'
      or date_trunc('month', acct_registration_complete_datetime) = '2018-02-01'
      or date_trunc('month', acct_registration_complete_datetime) = '2018-04-01'
         and datediff('day', acct_registration_complete_datetime, reportpulldate) >= 60;

-- 658008
--total
819597
821799





select registration_day
, case when time_lapse between 0 and 7 then 'week 1'
when time_lapse between 8 and 14 then 'week 2'
when time_lapse between 15 and 21 then 'week 3'
when time_lapse between 22 and 30 then 'week 4'
else null end as time_lapse
, count(distinct idhash)
from (
	select usertoken
	, idhash
	, date_trunc('day', acct_registration_complete_datetime) as registration_day
	, date_diff('day', acct_registration_complete_datetime, dateopened) as time_lapse
	from credit_report_tradelines
	join public.user using (usertoken)
	where 1
	and dateopened > acct_registration_complete_datetime
	and date_diff('day', acct_registration_complete_datetime, getdate()) between 60 and 250
	and creditor in (
			'CAP ONE','CAP ONE AUTO','CAPITAL ONE','CAP ONE MTG','CAP ONE NA','CAP1','CAP1 NA','CAP1/AMAPL','CAP1/ARTVN'
			,'CAP1/BALRD','CAP1/BDCOK','CAP1/BERGD','CAP1/BERGN','CAP1/BERPL','CAP1/BIGLT','CAP1/BIMRT','CAP1/BJS','CAP1/BLTNE'
			,'CAP1/BMBDR','CAP1/BONTN','CAP1/BOSCV','CAP1/BOSE','CAP1/BOSTN','CAP1/BRAD','CAP1/BRUNS','CAP1/BSTBY','CAP1/CARSN'
			,'CAP1/CASML','CAP1/CMPLT','CAP1/CNTRL','CAP1/COMP','CAP1/COMPQ','CAP1/COSCO','CAP1/DANA','CAP1/DAVBR','CAP1/DBARN'
			,'CAP1/DKNY','CAP1/DMARK','CAP1/DREXL','CAP1/EAGLE','CAP1/ECAPL','CAP1/ELDER','CAP1/ELISA','CAP1/ELLEN','CAP1/EQLIF'
			,'CAP1/EXCEL','CAP1/FLNES','CAP1/FRGTE','CAP1/FRNRW','CAP1/GARWT','CAP1/GATWY','CAP1/GLYNS','CAP1/GOTTS','CAP1/GRNHL'
			,'CAP1/GUITR','CAP1/HANKS','CAP1/HELTH','CAP1/HELZB','CAP1/HERBG','CAP1/HMKER','CAP1/IKEA','CAP1/IMPRV','CAP1/ISBRD'
			,'CAP1/JUSTICE','CAP1/KAWAS','CAP1/KMART','CAP1/KS','CAP1/L&T','CAP1/LEVTZ','CAP1/LIZ','CAP1/LUCKY','CAP1/MARCS'
			,'CAP1/MCRAE','CAP1/MGNOL','CAP1/MICRN','CAP1/MICRO','CAP1/MITSU','CAP1/MNRDS','CAP1/MOORE','CAP1/MUSIC','CAP1/NAUTL'
			,'CAP1/NEIMN','CAP1/NTHRN','CAP1/OFMAX','CAP1/PARSN','CAP1/PLNKT','CAP1/POLRS','CAP1/PRISC','CAP1/QVC','CAP1/RHODE'
			,'CAP1/RMSTR','CAP1/RS','CAP1/RTG','CAP1/SAKS','CAP1/SEAMN','CAP1/SMITH','CAP1/SONY','CAP1/STORE','CAP1/STRKY','CAP1/SUZKI'
			,'CAP1/TERAH','CAP1/TRVSM','CAP1/TSHBA','CAP1/TXTRN','CAP1/VENTR','CAP1/VLCTY','CAP1/VNCVR','CAP1/WBROS','CAP1/YMAHA'
			,'CAP1/YOUNK','CAP ONE/WOLF','KEY FOR CAP1','KOHLS/CAPONE'
			)
)
group by 1,2


