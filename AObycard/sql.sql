-- last 90 days
-- credit card only

drop table if exists one_offs.nz_cf;
create table one_offs.nz_cf
DISTKEY (usertoken)
as
 select 
 usertoken
  ,clickid
  ,createdate
  ,case when partner ilike 'cap%one' then 'CAPITAL_ONE'
  when partner ilike 'lendin%tree' then 'LENDING_TREE'
  when partner ilike 'pacific_credit_group' then 'PACIFIC_CREDIT_GROUP'
  when partner ilike 'quin%' then 'QUINSTREET'
  when partner ilike 'genesis' then 'GENESIS_BANKCARD_SERVICES'
  when partner ilike 'chase' then 'CHASE'
  when partner ilike 'car%direct' then 'CARSDIRECT'
  when partner ilike 'discover' then 'DISCOVER'
  when partner ilike 'lexington%' then 'LEXINGTON_LAW'
  when partner ilike '%trust' then 'MID_AMERICA_BANK_TRUST' --???!!! then ''
  when partner ilike 'credit%one%' then 'CREDIT_ONE_BANK'
  when partner ilike 'bankrate' then 'BANKRATE_CREDIT_CARD'
  when partner ilike 'one%main' then 'ONEMAIN'
  when partner ilike 'barclay%' then 'BARCLAYCARD'
  when partner ilike 'american%' then 'AMERICAN_EXPRESS'
  when partner ilike 'lendin%club' then 'LENDING_CLUB'
  when partner ilike '%even%financial%' then 'EVEN_FINANCIAL'
  when partner ilike '%avant%' then 'AVANT'
  when partner ilike '%indigo%' then 'INDIGO'
  when partner ilike '%netcredit%' then 'NETCREDIT'
  when partner ilike '%springleaf%' then 'SPRINGLEAF'
  when partner ilike '%opp%loan%' then 'OPP_LOANS'
  when partner ilike '%lend%up%' then 'LEND_UP'
  when partner ilike '%master%card%' then 'MASTERCARD'
  when partner ilike '%bank%of%america%' then 'BANK_OF_AMERICA'
  when partner ilike '%first%premier%' then 'FIRST_PREMIER'
  when partner ilike '%bank%rate%' then 'BANK_RATE_INC'
  when partner ilike '%rise%' then 'RISE'
  when partner ilike '%citi%' then 'CITI'
  when partner ilike '%prosper%' then 'PROSPER'
  when partner ilike '%net%credit%' then 'NET_CREDIT'
  when partner ilike '%upstart%' then 'UPSTART'
  when partner ilike '%emporium%' then 'EMPORIUM'
  when partner ilike '%wells%fargo%' then 'WELLS_FARGO'
  when partner ilike '%pcg%' then 'PCG'
  when partner ilike '%accredited%debt%relief%' then 'ACCREDITED_DEBT_RELIEF'
  ELSE upper(partner)
  end as partner

  
  , case when card_name ilike 'Cap%One%Platinum%' then 'Capital One Platinum Credit Card'
    when (card_name ilike '%quicksilver%' and card_name ilike '%one%one%') then 'Capital One QuicksilverOne Cash Rewards Credit Card'
    when card_name ilike 'Indigo%unsecured%' then 'Indigo Unsecured Mastercard'
    when card_name ilike 'Indigo%Platinum%' then 'Indigo Platinum Mastercard'
    when (card_name ilike 'amex%' or (card_name ilike '%express%' and card_name ilike '%everyday%')) then 'The Amex EveryDay Credit Card'
    when product_offer in ('1007') then 'Wells Fargo Platinum Visa Card'
    when product_offer ='13216092' then 'Discover it Cash Back'
    when card_name ilike '%discover%balance%' then 'Discover it Balance Transfer'
    when product_offer in ('ChaseFreedom', '31376') then 'Chase Freedom'
    when (card_name ilike '%Venture%Rewards%'and card_name ilike '%one%one%') then 'Capital One VentureOne Rewards Credit Card'
    when card_name ilike '%citi%double%' then 'Citi Double Cash Card – 18 month BT offer'
    when product_offer ='31377' then 'Chase Freedom Unlimited'
    when card_name ilike 'Cap%One%Secured%' then 'Capital One Secured Mastercard'
    when (card_name ilike '%Venture%Rewards%' and card_name not ilike '%one%one%') then 'Capital One Venture Rewards Credit Card'
    when (card_name ilike '%quicksilver%' and card_name not ilike '%one%one%') then 'Capital One Quicksilver Card'
    else card_name
  end as card_name
  , product_offer
  ,cast(null as varchar(20)) as aos
  ,cast(null as varchar(20)) as application
  ,cast(null as int) as trx
  ,cast(null as varchar(200)) as trx_partner


from public.click_applies_properties
where date(createdate) >= current_date -90
and vertical ilike 'CREDIT%CARD'
and clickid in 
	( select clickid as id
	from  public.click_applies
	where propertyname ilike 'logic%'
	and propertyvalue = 'AOs' 
	group by 1)
group by 1,2,3,4,5,6,7,8,9
;





update one_offs.nz_cf
set aos = odds
from
(
  select clickid as id
  , case when propertyvalue ilike 'FAIR' then 'Fair'
	when propertyvalue ilike 'GOOD' then 'Good'
	when propertyvalue ilike 'Poor' then 'poor'
	when propertyvalue ilike 'VERY%GOOD' then 'Very Good'
	else null
	end as odds 
  from  public.click_applies
where propertyname ilike 'appro%'
  group by 1,2
) eventcat
where clickid = id ;




update one_offs.nz_cf
set application = odds
from
(
  SELECT
 	case when approved != true then 'dec'
 	when approved = true then 'app'
 	else 'pending'
 	end as odds
 	, clickid as id
	FROM revenue.approvals_declines_credit_cards
	where partner_applied_timestamp>= '2018-01-01'
  	group by 1,2
) eventcat
where clickid = id ;



update one_offs.nz_cf
set trx = odds
from
(
select
   clickid as id
  , 1 as odds
from revenue.all_partners_revenue_data
where 1
and verticalname ilike 'Credit%Cards'
and amountreported > 0
group by 1,2
) eventcat	x
where clickid = id ;




SELECT
  card_name
, partner
, aos
, count(distinct usertoken) as cnt
FROM one_offs.nz_cf
where aos is not null
group by 1,2,3
order by 4 desc




SELECT
  card_name
, partner
, aos
, count(distinct usertoken) as cnt
FROM one_offs.nz_cf
where aos is not null
and  trx is not null
group by 1,2,3
order by 4 desc




















