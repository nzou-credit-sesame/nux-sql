with prequal as
(select
date(action_timestamp) as action_dt
, usertoken
from user_action as ua
inner join page as o
on o.page_sid = ua.page_sid
where 1
AND action_type = 'VIEW_PAGE'
and page_name ilike 'PRE_QUAL_OFFERS'
and date(action_timestamp) >= '2017-01-01'
group by 1,2
),
-- getting a new tradeline within 30d of prequal
potential as (
	SELECT
	d.usertoken
	, dateopened as tradeline_open_dt
	, action_dt

	FROM prequal d
	inner join public.credit_report_tradelines crt
	on d.usertoken = crt.usertoken
	where f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'CREDIT_CARD'
	and crt.openclosed =  'Open'
	and (crt.creditor ilike 'CAPITAL ONE'  or crt.creditor ilike 'CAP1')
	and dateopened < action_dt + 30
	and dateopened >= action_dt
	group by 1,2,3
),
-- all the rev data
rev as
(
select
    coalesce(cap.usertoken, rev.usertoken) as usertoken
  , coalesce(cap.createdate, rev.timeclicked) as createdate
  , rev.clickid
from revenue.all_partners_revenue_data rev
left join public.click_applies_properties cap
on cap.clickid = rev.clickid
where coalesce(cap.createdate, rev.timeclicked) >= '2017-01-01'
and rev.amountreported > 0
and rev.vendorname = 'Capital One Credit Cards'
group by 1,2,3),

prequal_rev as (
select
p.usertoken
,action_dt
,tradeline_open_dt
,date(createdate) as rev_dt

from potential p
left join rev r
on p.usertoken = r.usertoken
and date(createdate) >= tradeline_open_dt - 1
and date(createdate) < tradeline_open_dt + 7
group by 1,2,3,4
)


SELECT
count(*)
FROM prequal_rev d
where rev_dt is null
limit 100
