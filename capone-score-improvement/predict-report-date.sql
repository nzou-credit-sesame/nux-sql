with rev_user as (
	select
		cap.usertoken
		,cap.clickid
		, date(coalesce(cap.createdate, rev.timeclicked)) as clickdate
		, date(partner_decision_timestamp) as decisiondate
		, case when cap.card_name ilike 'Cap%One%Platinum%' then 'Capital One Platinum Credit Card'
			when (cap.card_name ilike '%quicksilver%' and cap.card_name ilike '%one%one%') then 'Capital One QuicksilverOne Cash Rewards Credit Card'
			else null
			end as cardname
		, amountreported as rev
		, dateopened
		, state
		, min(datereported) as datereport

	from public.click_applies_properties cap
	inner join public.user
	using (usertoken)
	inner join revenue.approvals_declines_credit_cards app
	using (clickid)
	inner join revenue.all_partners_revenue_data rev
	using (clickid)
	inner join
	(
		select
		*
		from credit_report_tradelines
		where 1
		and accounttype ilike 'credit%'
		and (creditor ilike '%cap%1' or creditor ilike 'cap%one%')
		and creditor not ilike 'CAPSTONE%'
		and openclosed ilike 'open'
	) crt
	on cap.usertoken = crt.usertoken
	and crt.dateopened = date(coalesce(cap.createdate, rev.timeclicked))

	where 1
	and date(cap.createdate) >= current_date - 365
	and partner_applied_timestamp >= current_date - 365
	and app.approved = true
	and cap.vertical ilike 'CREDIT%CARDs'
	and cap.partner ilike 'cap%one'
	and verticalname ilike 'Credit%Cards'
	and paystatushistory is null
	--or paystatushistory = 'C')
	and amountreported > 0
	group by 1,2,3,4,5,6,7,8
),

doublecard as
	(
	select
		usertoken
		, dateopened
		, datereported
		, creditinfodate
		, creditor
		, accounttype
		, row_number() over (partition by usertoken, creditinfodate order by dateopened) as rn
	from credit_report_tradelines
	where 1
	and (creditor ilike '%cap%1' or creditor ilike 'cap%one%')
	and creditor not ilike 'CAPSTONE%'
	and designation = 'Individual'
	and openclosed ilike 'open'
	and creditinfodate >= current_date - 365
),

doublecard_user as (
	select
	usertoken
	, min(creditinfodate) as tccdate
	from doublecard
	where 1
	and rn >=2
	group by 1
),

data as (
	select
		t1.*
		, (datereport - dateopened) as diff
	from rev_user t1
	left join doublecard_user t2
	on t1.usertoken = t2.usertoken
	where 1
	and rev is not null
	and cardname is not null
	--and t2.usertoken is null
	and (datereport - dateopened) <=62
)



-- narrow down day of the month effect
select
extract(day from dateopened)
, ttl
, count(*) as cnt
, cast(count(*) as decimal(18,5))/ttl as perc
from data,
(select count(*) as ttl from data
  where diff between 26 and 30)
where diff between 26 and 30
group by 1,2



select
extract(day from datereport)
, ttl
, count(*) as cnt
, cast(count(*) as decimal(18,5))/ttl as perc
from data,
(select count(*) as ttl from data
  where diff between 40 and 44)
where diff between 40 and 44
group by 1,2



-- narrow down weekday effect
select
diff
,datepart(dw, datereport) as weekday
, count(*) as cnt
from data
where diff between 26 and 30
group by 1,2


-- click apply difference
select diff
, count(*)
from data
group by 1

--  do 41-50 days have longer decision cycle
select
case when diff<= 30 then '<=30d'
	when diff<=40 then '31-40'
	when diff<50 then '41-50'
	else '50+'
		end as diff_g
,(decisiondate-dateopened) as dec_diff
, count(*) as cnt
, cast(count(*) as decimal(18,5))/avg(ttl) as perc
from data,
(select count(*) as ttl from data)
group by 1,2
order by 4

-- check weekday percentage
select 
datepart(dw, datereport) as weekday
, ttl
, count(*) as cnt
, cast(count(*) as decimal(18,5))/ttl as perc
from data,
(select count(*) as ttl from data)
group by 1,2























