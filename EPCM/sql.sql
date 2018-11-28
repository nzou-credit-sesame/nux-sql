with impression as (
	select
	usertoken
	, date(action_timestamp) as action_dt
	, case when (card_name ilike 'Capital%One%Venture%Rewards%' and card_name not ilike '%one%one%') then 'Capital One Venture Rewards Credit Card'
	 when (card_name ilike 'Capital%One%Venture%Rewards%' and card_name ilike '%one%one%') then 'Capital One VentureOne Rewards Credit Card'
	 when (card_name ilike 'Capital%One%quicksilver%' and card_name not ilike '%one%one%') then 'Capital One Quicksilver'
	 when (card_name ilike 'discover%balance%' and card_name not ilike '%student%') then 'Discover it Balance Transfer'
	 when (card_name ilike 'discover%cash%' and card_name not ilike '%student%') then 'Discover it Cash Back'
	 when card_name ilike 'Chase%Freedom%unlimited%' then 'Chase Freedom Unlimited'
	 when card_name ilike 'Chase%Freedom%' then 'Chase Freedom'
	 when card_name ilike '%amex%' then 'The Amex EveryDay Credit Card'
	 when card_name ilike '%citi%double%' then 'Citi Double Cash Card  18 month BT offer'
	 else null
	 end card_name
	from user_action as ua
	left join offer as o
	on o.offer_sid = ua.offer_sid
	left join page as p
	on p.page_sid = ua.page_sid
	where 1
	and o.offer_position =1
	and o.vertical ilike 'Cred%Card%'
	AND action_type = 'VIEW_OFFER'
	and date(action_timestamp) >= '2018-01-01'
	and page_name ilike '%aoop%'
),

clickapplies as (
select
p.card_name
,p.usertoken
, action_dt
,case when (cap.card_name ilike '%Venture%' and cap.card_name not ilike '%one%one%') then 'Capital One Venture Rewards Credit Card'
	 when (cap.card_name ilike '%Venture%' and cap.card_name ilike '%one%one%') then 'Capital One VentureOne Rewards Credit Card'
	 when (cap.card_name ilike '%quicksilver%' and cap.card_name not ilike '%one%one%') then 'Capital One Quicksilver'
	 when (cap.card_name ilike 'discover%balance%' and cap.card_name not ilike '%student%') then 'Discover it Balance Transfer'
	 when (cap.card_name ilike 'discover%cash%' and cap.card_name not ilike '%student%') then 'Discover it Cash Back'
	 when cap.card_name ilike 'Chase%Freedom%unlimited%' then 'Chase Freedom Unlimited'
	 when cap.card_name ilike 'Chase%Freedom%' then 'Chase Freedom'
	 when cap.card_name ilike '%amex%' then 'The Amex EveryDay Credit Card'
	 when cap.card_name ilike '%citi%double%' then 'Citi Double Cash Card  18 month BT offer'
	 else null
	 end cp_card_name
, clickid
from impression p
inner join public.click_applies_properties cap
on p.usertoken = cap.usertoken
where p.card_name is not null
and date(createdate) = action_dt
and page_name ilike '%aoop%'
group by 1,2,3,4,5
),

rev as (
select
    coalesce(cap.usertoken, rev.usertoken) as usertoken
  , rev.clickid
  , cap.card_name
from revenue.all_partners_revenue_data rev
inner join clickapplies cap
on cap.clickid = rev.clickid
where 1
and cap.card_name = cp_card_name
and rev.amountreported > 0
group by 1,2,3
)



select
card_name
, count(usertoken)
from rev
group by 1
order by 1



-- aoop
-- ocf

select
card_name
, count(distinct usertoken)
from clickapplies p
where card_name is not null
and card_name = cp_card_name
group by 1
order by 1



select 
card_name
, count(usertoken)
from impression
where card_name is not null
group by 1
order by 1





with clickapplies as (
select
	usertoken
	,case when (cap.card_name ilike '%Venture%' and cap.card_name not ilike '%one%one%') then 'Capital One Venture Rewards Credit Card'
	 when (cap.card_name ilike '%Venture%' and cap.card_name ilike '%one%one%') then 'Capital One VentureOne Rewards Credit Card'
	 when (cap.card_name ilike '%quicksilver%' and cap.card_name not ilike '%one%one%') then 'Capital One Quicksilver'
	 when (cap.card_name ilike 'discover%balance%' and cap.card_name not ilike '%student%') then 'Discover it Balance Transfer'
	 when (cap.card_name ilike 'discover%cash%' and cap.card_name not ilike '%student%') then 'Discover it Cash Back'
	 when cap.card_name ilike 'Chase%Freedom%unlimited%' then 'Chase Freedom Unlimited'
	 when cap.card_name ilike 'Chase%Freedom%' then 'Chase Freedom'
	 when cap.card_name ilike '%amex%' then 'The Amex EveryDay Credit Card'
	 when cap.card_name ilike '%citi%double%' then 'Citi Double Cash Card  18 month BT offer'
	 else null
	 end cp_card_name
	, clickid
from public.click_applies_properties cap
where 1
and page_name ilike '%aoop%'
and createdate >= '2018-01-01'
and offer_position =1
group by 1,2,3
)

select
cp_card_name
, count(*)
from clickapplies p
where cp_card_name is not null
group by 1
order by 1





with clickapplies as (
select
	usertoken
	,case when (cap.card_name ilike '%Venture%' and cap.card_name not ilike '%one%one%') then 'Capital One Venture Rewards Credit Card'
	 when (cap.card_name ilike '%Venture%' and cap.card_name ilike '%one%one%') then 'Capital One VentureOne Rewards Credit Card'
	 when (cap.card_name ilike '%quicksilver%' and cap.card_name not ilike '%one%one%') then 'Capital One Quicksilver'
	 when (cap.card_name ilike 'discover%balance%' and cap.card_name not ilike '%student%') then 'Discover it Balance Transfer'
	 when (cap.card_name ilike 'discover%cash%' and cap.card_name not ilike '%student%') then 'Discover it Cash Back'
	 when cap.card_name ilike 'Chase%Freedom%unlimited%' then 'Chase Freedom Unlimited'
	 when cap.card_name ilike 'Chase%Freedom%' then 'Chase Freedom'
	 when cap.card_name ilike '%amex%' then 'The Amex EveryDay Credit Card'
	 when cap.card_name ilike '%citi%double%' then 'Citi Double Cash Card  18 month BT offer'
	 else null
	 end cp_card_name
	, clickid
from public.click_applies_properties cap
where 1
and page_name ilike '%aoop%'
and createdate >= '2018-01-01'
and offer_position =1
group by 1,2,3
),

rev as (
select
    coalesce(cap.usertoken, rev.usertoken) as usertoken
  , rev.clickid
  , cp_card_name
  , amountreported
from revenue.all_partners_revenue_data rev
inner join clickapplies cap
on cap.clickid = rev.clickid
where 1
and rev.amountreported > 0
group by 1,2,3,4
)

select
cp_card_name
, count(*)
, avg(amountreported)
from rev
group by 1
order by 1




------------- ------------- 
------------- ------------- 
------------- ------------- 
------------- OCF ---------
------------- ------------- 
------------- ------------- 
------------- ------------- 

with clickapplies as (
select
	usertoken
	,case when (cap.card_name ilike '%Venture%' and cap.card_name not ilike '%one%one%') then 'Capital One Venture Rewards Credit Card'
	 when (cap.card_name ilike '%Venture%' and cap.card_name ilike '%one%one%') then 'Capital One VentureOne Rewards Credit Card'
	 when (cap.card_name ilike '%quicksilver%' and cap.card_name not ilike '%one%one%') then 'Capital One Quicksilver'
	 when (cap.card_name ilike 'discover%balance%' and cap.card_name not ilike '%student%') then 'Discover it Balance Transfer'
	 when (cap.card_name ilike 'discover%cash%' and cap.card_name not ilike '%student%') then 'Discover it Cash Back'
	 when cap.card_name ilike 'Chase%Freedom%unlimited%' then 'Chase Freedom Unlimited'
	 when cap.card_name ilike 'Chase%Freedom%' then 'Chase Freedom'
	 when cap.card_name ilike '%amex%' then 'The Amex EveryDay Credit Card'
	 when cap.card_name ilike '%citi%double%' then 'Citi Double Cash Card  18 month BT offer'
	 else null
	 end cp_card_name
	, clickid
from public.click_applies_properties cap
where 1
and createdate >= '2018-01-01'

and offer_position =1
and site ilike 'public'
and page_name not ilike 'aoop%'
and page_name not ilike 'cc best cards'
and page_name not ilike 'my borrowing power'
and page_name not ilike 'PL Marketplace'
group by 1,2,3
)

select
cp_card_name
, count(*)
from clickapplies p
where cp_card_name is not null
group by 1
order by 1





