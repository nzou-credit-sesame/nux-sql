create temp table last_10_ca
as (
 select cap.usertoken
  ,cap.clickid
  ,cap.createdate
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
    when card_name ilike '%amex%' then 'The Amex EveryDay Credit Card'
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
    else null
  end as card_name
from public.click_applies_properties cap
where date(createdate) >= current_date -10
and vertical ilike 'CREDIT%CARD'
group by 1,2,3,4,5
  );


drop table if exists one_offs.nz_cf;
create table one_offs.nz_cf
DISTKEY (usertoken)
as
select * 
from (
  select
    l.*
    , case
        when cph.vantage3 is null then 0
        when cph.vantage3 < 300 then 0
        else cph.vantage3
        END as vantage3
    , row_number() over (
        partition by cph.usertoken
        ORDER BY cph.reportpulldate asc nulls last, cph.creditinfodate asc nulls last, cph.creditinfoid asc nulls last
        ) as rn

    from 
    (
      select * 
      from last_10_ca
      where card_name is not null
    ) l
    inner join public.credit_profile_history cph
    on l.usertoken= cph.usertoken
   ) x
where rn = 1
;

71067




select 
coalesce(vantage3 - (vantage3 % 50), 0) as band
, card_name
, count(distinct clickid)
from one_offs.nz_cf
group by 1,2
  
  
  
  
  
  
  




