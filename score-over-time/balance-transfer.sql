-- qunatiles
-- mean, median, max, min, 20,30,90
--2832	2000.0	35000	200	700.0, 1000.0	5800.0



--17k
select
count(distinct cap.usertoken)
from revenue.all_partners_revenue_data rev
inner join public.click_applies_properties cap
on cap.clickid = rev.clickid
inner join public.credit_report_tradelines crt
on crt.usertoken = cap.usertoken
where cap.card_name ilike '%Chase%Slate%'
and crt.creditor ilike '%chase%'
and dateopened >= cast(cap.createdate as date)
and dateopened <= cast(cap.createdate as date)+2
and openclosed ilike 'Open'
and f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'CREDIT_CARD'





-- 506k
-- WITH work as VIEW
create temp table potential as (
SELECT
	curr.usertoken
	, max(curr.reportpulldate) as curr_report_date
	, min(next.reportpulldate) as next_report_date

FROM one_offs.rb_pivot  AS curr
INNER JOIN one_offs.rb_pivot  AS next
ON next.usertoken = curr.usertoken
and (DATE(curr.reportpulldate )) < (DATE(next.reportpulldate ))

WHERE (floor(months_between((DATE(next.reportpulldate )), (DATE(curr.reportpulldate ))))  >= 1
AND floor(months_between((DATE(next.reportpulldate )), (DATE(curr.reportpulldate ))))  <= 11)
AND curr.totalaccounts = (next.totalaccounts - 1)
AND next.delinquentaccounts  <= curr.delinquentaccounts
AND next.derogatoryaccounts  <= curr.derogatoryaccounts
AND (next.cc_count - curr.cc_count ) = 1
and curr.total_balance > 0
AND abs(next.total_balance- curr.total_balance)/ curr.total_balance <= 0.1
AND (next.total_limit  - curr.total_limit ) <= 5800
AND (next.total_limit  - curr.total_limit ) >= 1000
group by 1
);





--52k without date filter
-- 22k with 4months window
drop table if exists one_offs.nz_btcard;
create table one_offs.nz_btcard as (
select
crt.usertoken
,curr_report_date
,next_report_date

from potential p
inner join public.credit_report_tradelines crt
on p.usertoken = crt.usertoken
and dateopened >= curr_report_date
inner join public.credit_profile_history cph
on crt.usertoken = cph.usertoken
and crt.creditinfoid = cph.creditinfoid
and reportpulldate > curr_report_date
and reportpulldate <= next_report_date
where f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'CREDIT_CARD'
and crt.openclosed =  'Open'
and crt.currentbalance >= crt.creditlimit*0.6
and (next_report_date - curr_report_date) <= 120
group by 1,2,3
);


--- validating part
select count(distinct usertoken) from one_offs.nz_btcard 


select
(next_report_date - curr_report_date)/30 as month_out
, count(distinct p.usertoken) as cnt
from  one_offs.nz_btcard p
group by 1


select
coalesce(cph.vantage3 - (cph.vantage3 % 50), 0) as band
, count(distinct p.usertoken) as cnt
from btcard p
inner join public.credit_report_tradelines crt
on p.usertoken = crt.usertoken
inner join public.credit_profile_history cph
on crt.usertoken = cph.usertoken
and crt.creditinfoid = cph.creditinfoid
where f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'CREDIT_CARD'
and reportpulldate >= curr_report_date
and reportpulldate < next_report_date
and crt.openclosed =  'Open'
group by 1




# exsiting card balance comparison
select
crt.usertoken
,reportpulldate
,sum(crt.currentbalance) as sum_bal
from public.credit_report_tradelines crt
inner join public.credit_profile_history cph
on crt.usertoken = cph.usertoken
and crt.creditinfoid = cph.creditinfoid

inner join one_offs.nz_btcard p
on p.usertoken = crt.usertoken
and reportpulldate >= curr_report_date
and reportpulldate <= next_report_date
and dateopened < curr_report_date
where f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'CREDIT_CARD'
and crt.openclosed =  'Open'
group by 1,2
limit 100



-- number of collections
-- https://goo.gl/eZ9tgZ
-- 0-1
-- payment history
-- all kinds

select
floor(months_between((DATE(next.reportpulldate )), (DATE(curr.reportpulldate ))))  AS months_since_last_pull
, curr.vantage3 - (curr.vantage3 % 50)  AS credit_score_band
,CASE WHEN curr.collection_count<1 THEN curr.collection_count
WHEN curr.collection_count>=1 THEN 1
else null
end as current_collection_count
, COUNT(DISTINCT curr.usertoken ) AS distinct_users
from one_offs.nz_btcard p
inner join one_offs.rb_pivot  AS curr
on p.usertoken = curr.usertoken
and DATE(curr.reportpulldate ) >= curr_report_date

INNER JOIN one_offs.rb_pivot  AS next
ON next.usertoken = curr.usertoken
and (DATE(curr.reportpulldate )) < (DATE(next.reportpulldate ))

WHERE (floor(months_between((DATE(next.reportpulldate )), (DATE(curr.reportpulldate ))))  >= 1
AND floor(months_between((DATE(next.reportpulldate )), (DATE(curr.reportpulldate ))))  <= 11)
AND curr.totalaccounts = (next.totalaccounts - 1)
AND next.delinquentaccounts  <= curr.delinquentaccounts
AND next.derogatoryaccounts  <= curr.derogatoryaccounts
AND (next.cc_count - curr.cc_count ) = 1
and curr.total_balance > 0
and curr.vantage3 >=600

AND curr.credit_utilization_ratio >= 0.7
AND next.credit_utilization_ratio < curr.credit_utilization_ratio
AND next.credit_utilization_ratio >= 0.5

group by 1,2,3




select
floor(months_between((DATE(next.reportpulldate )), (DATE(curr.reportpulldate ))))  AS months_since_last_pull
, curr.vantage3 - (curr.vantage3 % 50)  AS credit_score_band
,CASE WHEN curr.collection_count<1 THEN curr.collection_count
WHEN curr.collection_count>=1 THEN 1
else null
end as current_collection_count
,AVG(next.vantage3::float - curr.vantage3::float ) AS average_vantage3_change
,PERCENTILE_CONT(0.2) WITHIN GROUP (ORDER BY next.vantage3::float - curr.vantage3::float) AS percentile_20
,PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY next.vantage3::float - curr.vantage3::float) AS percentile_80



from one_offs.nz_btcard p
inner join one_offs.rb_pivot  AS curr
on p.usertoken = curr.usertoken
and DATE(curr.reportpulldate ) >= curr_report_date

INNER JOIN one_offs.rb_pivot  AS next
ON next.usertoken = curr.usertoken
and (DATE(curr.reportpulldate )) < (DATE(next.reportpulldate ))

WHERE (floor(months_between((DATE(next.reportpulldate )), (DATE(curr.reportpulldate ))))  >= 1
AND floor(months_between((DATE(next.reportpulldate )), (DATE(curr.reportpulldate ))))  <= 11)
AND curr.totalaccounts = (next.totalaccounts - 1)
AND next.delinquentaccounts  <= curr.delinquentaccounts
AND next.derogatoryaccounts  <= curr.derogatoryaccounts
AND (next.cc_count - curr.cc_count ) = 1
and curr.total_balance > 0
and curr.vantage3 >=600

AND curr.credit_utilization_ratio <= 0.7 
AND curr.credit_utilization_ratio >= 0.3
AND  next.credit_utilization_ratio <= curr.credit_utilization_ratio
AND next.credit_utilization_ratio >= 0.3


group by 1,2,3



-- F
AND curr.credit_utilization_ratio >= 0.7
AND next.credit_utilization_ratio < curr.credit_utilization_ratio
AND next.credit_utilization_ratio >= 0.5
-- C-D
AND curr.credit_utilization_ratio <= 0.7 
AND curr.credit_utilization_ratio >= 0.3
AND  next.credit_utilization_ratio <= curr.credit_utilization_ratio
AND next.credit_utilization_ratio >= 0.3

-- A-B
AND curr.credit_utilization_ratio  <= 0.3




--https://goo.gl/x3PCpG




