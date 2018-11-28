create temp table nz_user_profile
DISTKEY (usertoken)
as
(
  select
  *
  from
  (
    select
    u.usertoken
    , u.acct_registration_complete_datetime
    , date_trunc('month', u.acct_registration_complete_datetime) as cohort
    , coalesce(cph.vantage3 - (cph.vantage3 % 50), 0) as band
    , mstc.marketingchannel
    , acct_registration_completed_session_id
    , case
    when cph.vantage3 is null then 0
    when cph.vantage3 < 300 then 0
    else cph.vantage3
    END as vantage3
    , row_number() over (
    partition by u.usertoken
    ORDER BY cph.reportpulldate asc nulls last, cph.creditinfodate asc nulls last, cph.creditinfoid asc nulls last
    ) as rn
    from public."user" u
    left join public.credit_profile_history cph
    on cph.usertoken = u.usertoken
    left join one_offs.marketing_source_to_channel mstc
    on mstc.marketingsource = u.marketing_source
    where not u.test_ssn_user
    and u.acct_registration_complete_datetime is not null
    and cast(u.acct_registration_complete_datetime as date) >='2017-01-01'
  ) x
  where rn = 1
);
--check table: marketing_source_to_channel

create temp table nz_rev
DISTKEY (usertoken)
compound sortkey (usertoken, createdate)
as
(
select
    coalesce(cap.usertoken, rev.usertoken) as usertoken
  , coalesce(cap.createdate, rev.timeclicked) as createdate
  , date_trunc('month', coalesce(cap.createdate, rev.timeclicked)) as rev_cohort
  , cap.sessionid
  , rev.clickid
  , rev.amountreported
  , case
    when rev.verticalname is null then null
    when rev.verticalname = 'Credit Cards' then 'CREDIT_CARD'
    when rev.verticalname = 'MerchandiseFinancing' then 'MERCHENDISE_FINANCING'
    when rev.verticalname = 'advertising' then 'ADVERTISING'
    when rev.verticalname = 'auto' then 'AUTO'
    when rev.verticalname = 'banking' then 'BANKING'
    when rev.verticalname = 'credit_cards' then 'CREDIT_CARD'
    when rev.verticalname = 'credit_repair' then 'CREDIT_REPAIR'
    when rev.verticalname = 'mortgage' then 'MORTGAGE'
    when rev.verticalname = 'personal_loans' then 'PERSONAL_LOAN'
    when rev.verticalname = 'subscription' then 'SUBSCRIPTION'
    when rev.verticalname is not null then '' || upper(rev.verticalname)
    end as vertical
  , case
    when rev.vendorname is null then null
    when rev.vendorname = 'American Express Consumer Affiliate' then 'AMERICAN_EXPRESS'
    when rev.vendorname = 'Bank of America - Credit Cards' then 'BANK_OF_AMERICA'
    when rev.vendorname = 'Barclaycard' then 'BARCLAYCARD'
    when rev.vendorname = 'Capital Bank' then 'CAPITAL_BANK'
    when rev.vendorname = 'Capital One Auto Finance' then 'CAPITAL_ONE'
    when rev.vendorname = 'Capital One Credit Cards' then 'CAPITAL_ONE'
    when rev.vendorname = 'Chase' then 'CHASE'
    when rev.vendorname = 'Citi Credit Cards' then 'CITI_CREDIT_CARDS'
    when rev.vendorname = 'Credit One Bank' then 'CREDIT_ONE_BANK'
    when rev.vendorname = 'Discover' then 'DISCOVER'
    when rev.vendorname = 'Discover Card' then 'DISCOVER'
    when rev.vendorname = 'Fifth Third Bank' then 'FIFTH_THIRD_BANK'
    when rev.vendorname = 'First Progress' then 'FIRST_PROGRESS'
    when rev.vendorname = 'Genesis Bankcard Services' then 'GENESIS_BANKCARD_SERVICES'
    when rev.vendorname = 'JP Morgan Chase (Credit Cards)' then 'CHASE'
    when rev.vendorname = 'Mid America Bank  Trust Company' then 'MID_AMERICA_BANK_TRUST_COMPANY'
    when rev.vendorname = 'Mid America Bank & Trust' then 'MID_AMERICA_BANK_TRUST'
    when rev.vendorname = 'The Bank of Missouri' then 'BANK_OF_MISSOURI'
    when rev.vendorname = 'Wells Fargo' then 'WELLS_FARGO'
    when rev.vendorname = 'accredited_debt_relief' then 'ACCREDITED_DEBT_RELIEF'
    when rev.vendorname = 'avant' then 'AVANT'
    when rev.vendorname = 'bankrate_credit_card' then 'BANKRATE_CREDIT_CARD'
    when rev.vendorname = 'carsdirect' then 'CARSDIRECT'
    when rev.vendorname = 'chase' then 'CHASE'
    when rev.vendorname = 'elevate' then 'ELEVATE'
    when rev.vendorname = 'lcard' then 'LCARD'
    when rev.vendorname = 'lendingclub' then 'LENDINGCLUB'
    when rev.vendorname = 'lendingclub2' then 'LENDINGCLUB'
    when rev.vendorname = 'lendingpoint' then 'LENDINGPOINT'
    when rev.vendorname = 'lendingtree' then 'LENDINGTREE'
    when rev.vendorname = 'lendingtree2' then 'LENDINGTREE'
    when rev.vendorname = 'lendup' then 'LENDUP'
    when rev.vendorname = 'marcus' then 'MARCUS'
    when rev.vendorname = 'nationaldebtrelief' then 'NATIONALDEBTRELIEF'
    when rev.vendorname = 'netcredit' then 'NETCREDIT'
    when rev.vendorname = 'onemain' then 'ONEMAIN'
    when rev.vendorname = 'opploans' then 'OPPLOANS'
    when rev.vendorname = 'pacificcreditgroup' then 'PACIFIC_CREDIT_GROUP'
    when rev.vendorname = 'prosper' then 'PROSPER'
    when rev.vendorname = 'prosper2' then 'PROSPER'
    when rev.vendorname = 'quinstreet_pl' then 'QUINSTREET'
    when rev.vendorname = 'quinstreet_pl2' then 'QUINSTREET'
    when rev.vendorname = 'total_attorney' then 'TOTAL_ATTORNEY'
    when rev.vendorname = 'upstart' then 'UPSTART'
    when rev.vendorname ilike '%U.S%BANK%' then 'US_BANK'
    when rev.vendorname = 'lexingtonlaw' then 'LEXINGTON_LAW'
    else upper(rev.vendorname)
    end as partner
    ,cap.mailing_id
    ,cap.page_name
    ,cast(null as varchar(20)) as site
    ,cap.ref_code
    ,cast(null as varchar(20)) as feature

from revenue.all_partners_revenue_data rev
left join public.click_applies_properties cap
on cap.clickid = rev.clickid
where coalesce(cap.createdate, rev.timeclicked) >= '2017-01-01'
and rev.amountreported > 0
);


update nz_rev
set site = odds
from
(
  select clickid as id
  , propertyvalue as odds
  from  public.click_applies cap1
  where cap1.propertyname ilike 'site'
  group by 1,2
) eventcat
where clickid = id ;


update nz_rev
set feature = 'ORGANIC_PRODUCT'
where mailing_id is null 
and (ref_code is null or ref_code ilike '%logged%out%')


update nz_rev
set feature = 'DIRECT_TO_PARTNER'
where mailing_id is not null 

update nz_rev
set feature = 'INTERNAL_EMAIL'
where ref_code is not null
and mailing_id is null


update nz_rev
set feature = 'OCF'
where site ilike 'public'
and ref_code not ilike '%logged%out%'
and page_name not ilike 'aoop%'
and page_name not ilike 'cc best cards'
and page_name not ilike 'my borrowing power'
and page_name not ilike 'PL Marketplace'



drop table if exists one_offs.nz_trx;
create table one_offs.nz_trx
DISTKEY (usertoken)
COMPOUND SORTKEY (band, cohort)
as
select
  up.usertoken
, up.band
, up.cohort
, up.acct_registration_complete_datetime
, up.marketingchannel
, rev.clickid
, rev.createdate
, rev.vertical
, rev.partner
, rev.amountreported
, rev.feature
, rev.page_name
from nz_user_profile up
left join nz_rev rev
on up.usertoken = rev.usertoken
and rev.rev_cohort = up.cohort
--and up.acct_registration_complete_datetime>= '2018-01-01'
group by 1,2,3,4,5,6,7,8,9,10,11,12
;
GRANT SELECT ON TABLE one_offs.nz_trx TO looker;







drop table if exists one_offs.nz_trx_1;
create table one_offs.nz_trx_1
DISTKEY (usertoken)
COMPOUND SORTKEY (band, cohort)
as
select
  up.usertoken
, up.band
, up.cohort
, rev.feature
from nz_user_profile up
left join nz_rev rev
on up.usertoken = rev.usertoken
and rev.rev_cohort = up.cohort
and rev.sessionid <> acct_registration_completed_session_id
group by 1,2,3,4
;
GRANT SELECT ON TABLE one_offs.nz_trx_1 TO looker;



--------------------------------------
--------------------------------------
--------------------------------------
--------------------------------------
--------------------------------------
--------------------------------------
-- click applies analysis
--------------------------------------
--------------------------------------
--------------------------------------
--------------------------------------
--------------------------------------
--------------------------------------
drop table if exists one_offs.nz_click;
create table one_offs.nz_click
DISTKEY (usertoken)
COMPOUND SORTKEY (band, cohort)
as
  select up.usertoken
  ,up.band
  ,up.cohort
  ,up.acct_registration_complete_datetime
  ,cap.clickid
  ,up.marketingchannel
  ,cap.createdate
  ,cap.vertical
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

from nz_user_profile up
left join public.click_applies_properties cap
on up.usertoken = cap.usertoken
and up.cohort = date_trunc('month', cap.createdate)
group by 1,2,3,4,5,6,7,8,9
;
GRANT SELECT ON TABLE one_offs.nz_click TO looker;

--------------------------------------
--------------------------------------
--------------------------------------
--------------------------------------
--------------------------------------
--------------------------------------
-- approval analysis
--------------------------------------
--------------------------------------
--------------------------------------
--------------------------------------
--------------------------------------
--------------------------------------
create temp table nz_app
DISTKEY (usertoken)
compound sortkey (usertoken, createdate)
as
(
SELECT
      coalesce(cap.usertoken, app.usertoken) as usertoken
    , coalesce(cap.createdate, app.partner_applied_timestamp) as createdate
    , date_trunc('month', coalesce(cap.createdate, app.partner_applied_timestamp)) as rev_cohort
    , app.approved
    , app.clickid
    , case when app.partner ilike 'American Express%' then upper('American_Express')
          when app.partner ilike 'Bank of America%' then upper('Bank_of_America')
          when app.partner ilike 'Barclaycard' then upper('Barclay')
          when app.partner ilike '%Capital%One%' then upper('Capital_One')
          when app.partner ilike 'Citi%' then upper('CITI')
          when app.partner ilike 'Discover%' then upper('DISCOVER')
          when app.partner ilike 'Fifth Third Bank' then upper('Fifth_Third_Bank')
          when app.partner ilike 'HSBC%' then upper('HSBC')
          when app.partner ilike 'chase' then upper('CHASE')
          when app.partner ilike 'flexshopper' then upper('FLEXSHOPPER')
          when app.partner ilike 'genesis' then upper('GENESIS')
          when app.partner ilike 'lcard' then upper('LCARD')
          when app.partner ilike 'wells%fargo' then upper('WELLS_FARGO')
          else upper(app.partner)
          end as partner
FROM revenue.approvals_declines_credit_cards  AS app
left join click_applies_properties cap
ON cap.clickid = app.clickid
where app.partner_applied_timestamp>= '2017-01-01'
);

drop table if exists one_offs.nz_app;
create table one_offs.nz_app
DISTKEY (usertoken)
COMPOUND SORTKEY (band, cohort)
as
select
  up.usertoken
, up.band
, up.cohort
, up.acct_registration_complete_datetime
, up.vantage3
, rev.clickid
, rev.createdate
, rev.partner
, rev.approved
from nz_user_profile up
left join nz_app rev
on up.usertoken = rev.usertoken
and rev.rev_cohort = up.cohort
group by 1,2,3,4,5,6,7,8,9
;
GRANT SELECT ON TABLE one_offs.nz_app TO looker;