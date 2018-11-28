select
page_name as page
, count(distinct usertoken)
from user_action as ua
inner join page as o
on o.page_sid = ua.page_sid
where 1
AND action_type = 'VIEW_PAGE'
and (page_name ilike 'PRE_QUAL_OFFERS' or page_name ilike 'NO_PRE_QUAL_OFFERS')
and date(action_timestamp) >= '2017-01-01'
group by 1



with prequal as
(select
date(action_timestamp) as action_dt
, page_name as page
, usertoken
from user_action as ua
inner join page as o
on o.page_sid = ua.page_sid
where 1
AND action_type = 'VIEW_PAGE'
and (page_name ilike 'PRE_QUAL_OFFERS' or page_name ilike 'NO_PRE_QUAL_OFFERS')
and date(action_timestamp) >= '2017-01-01'
group by 1,2,3),

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
group by 1,2,3)



select
page
, (date(createdate) - action_dt)/30 as cohort
, count(distinct p.usertoken)
from prequal p
inner join rev r
on p.usertoken = r.usertoken
and date(createdate) >= action_dt
and date(createdate) < action_dt +180
group by 1,2
order by 3 desc


select
page
, (date(createdate) - action_dt)/30 as cohort
, count(distinct p.usertoken)
from prequal p
inner join public.click_applies_properties cap
on p.usertoken = cap.usertoken
where  partner ilike 'cap%one'
and date(createdate) >= action_dt
and date(createdate) < action_dt +180
group by 1,2
order by 3 desc



SELECT
page
, (dateopened - action_dt)/30 as cohort
, count(distinct d.usertoken)
FROM prequal d
inner join public.credit_report_tradelines crt
on d.usertoken = crt.usertoken
where f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'CREDIT_CARD'
and crt.openclosed =  'Open'
and dateopened < action_dt + 180
and dateopened >= action_dt
and (crt.creditor ilike 'CAPITAL ONE'  or crt.creditor ilike 'CAP1')
group by 1,2
order by 3 desc





with prequal as
(select
date(action_timestamp) as action_dt
, page_name as page
, usertoken
from user_action as ua
inner join page as o
on o.page_sid = ua.page_sid
where 1
AND action_type = 'VIEW_PAGE'
and (page_name ilike 'PRE_QUAL_OFFERS' or page_name ilike 'NO_PRE_QUAL_OFFERS')
and date(action_timestamp) >= '2017-01-01'
group by 1,2,3),
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
page,
action_dt,
p.usertoken,
date(createdate) as cdt
from prequal p
inner join rev r
on p.usertoken = r.usertoken
and date(createdate) >= action_dt
and date(createdate) < action_dt +180
group by 1,2,3,4
)


SELECT
page
, (dateopened - action_dt)/30 as cohort
, count(distinct d.usertoken)
FROM prequal_rev d
inner join public.credit_report_tradelines crt
on d.usertoken = crt.usertoken
where f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'CREDIT_CARD'
and crt.openclosed =  'Open'
and dateopened < action_dt + 180
and dateopened >= action_dt
and dateopened < cdt + 7
and dateopened >= cdt
and (crt.creditor ilike 'CAPITAL ONE'  or crt.creditor ilike 'CAP1')
group by 1,2
order by 3 desc



