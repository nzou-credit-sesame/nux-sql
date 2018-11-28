create temp table declined as (
	SELECT
	    coalesce(cap.usertoken, app.usertoken) as usertoken
	    , cast(coalesce(cap.createdate, app.partner_applied_timestamp) as date) as application_date
	    , cap.card_name
	    , app.partner
	FROM revenue.approvals_declines_credit_cards  AS app
	left join click_applies_properties cap
	ON cap.clickid = app.clickid

	left join (
	SELECT
	    coalesce(cap.usertoken, app.usertoken) as usertoken
	    , cap.card_name
	    , app.partner
	FROM revenue.approvals_declines_credit_cards  AS app
	left join click_applies_properties cap
	ON cap.clickid = app.clickid
	where 1
 	and cap.vertical ilike 'CREDIT_CARD'
 	and approved = true
		) approved
	on coalesce(cap.usertoken, app.usertoken) =  approved.usertoken
	and cap.card_name = approved.card_name
	and cap.partner = approved.partner

	where 1
 	and cap.vertical ilike 'CREDIT_CARD'
 	and approved = false
    and approved.usertoken is null
 	and (cap.card_name not ilike '%secure%' or cap.card_name ilike '%unsecure%')
 	--and cast(coalesce(cap.createdate, app.partner_applied_timestamp) as date)>= '2017-01-01'
	group by 1,2,3,4
);



drop table if exists one_offs.nz_potential;
create table one_offs.nz_potential
DISTKEY (usertoken)
as
(
	SELECT
	d.usertoken
	, dateopened
	, application_date
	, (dateopened - application_date)/30 as cohort
	, card_name
	, case when crt.creditor ilike 'CAPITAL ONE%' then 'CAPITAL_ONE'
			when crt.creditor ilike 'CHASE CARD' then 'CHASE'
			when crt.creditor ilike 'CB INDIGO' then 'INDIGO'
			when crt.creditor ilike 'DISCOVERBANK' then 'DISCOVER'
			when crt.creditor ilike 'CREDITONEBNK' then 'CREDIT_ONE'
			when crt.creditor ilike 'BK OF AMER' then 'BOA'
			when crt.creditor ilike 'BANKAMERICA' then 'BOA'
			when crt.creditor ilike 'FST PREMIER' then 'FST_PREMIER'
			when crt.creditor ilike 'LENDUP CARD' then 'LENDUP'
			when crt.creditor ilike 'CAP1' then 'CAPITAL_ONE'
			ELSE crt.creditor
			end as creditor
	, case when partner ilike  'Capital One Credit Cards%' then 'CAPITAL_ONE'
			when partner ilike  'genesis' then 'INDIGO'
			when partner ilike  'chase' then 'CHASE'
			when partner ilike  'Discover Card' then 'DISCOVER'
			when partner ilike  'American Express Consumer Affiliate' then 'AMEX'
			when partner ilike  'lcard' then 'LENDUP'
			when partner ilike  'Bank of America - Credit Cards' then 'BOA'
			when partner ilike  'Citi Credit Cards' then 'CITI'
			when partner ilike  'Barclaycard' then 'BRCLYSBANKDE'
			when partner ilike  'American Express Consumer Cards' then 'AMEX'
			when partner ilike  'wells fargo' then 'WELLS FARGO'
			else partner
			end as partner
	,cast(null as date) as last_trade_line_dt

	FROM declined d
	left join 
		(
			select creditor
			, usertoken
			, dateopened 
			from public.credit_report_tradelines crt
			where f_credit_type_classifier(crt.accounttype, crt.credittype, crt.industry) = 'CREDIT_CARD'
			and crt.openclosed =  'Open'
			group by 1,2,3
		) crt
	on d.usertoken = crt.usertoken
	and dateopened < application_date + 180
	and dateopened > application_date
);

update one_offs.nz_potential
set last_trade_line_dt = odds
from
(
		select p.usertoken as ut
		, max(creditinfodate) as odds
		from declined p
		inner join public.credit_profile_history cph
		on p.usertoken = cph.usertoken
		and creditinfodate > application_date
		group by 1
) eventcat
where usertoken = ut ;


ALTER TABLE one_offs.nz_potential
add column rev_partner varchar(100);




update one_offs.nz_potential
set rev_partner  = partner_rev
from
(
select
    coalesce(cap.usertoken, rev.usertoken) as ut
  , date(coalesce(cap.createdate, rev.timeclicked)) as createdate
  , case
    when rev.vendorname is null then null
    when rev.vendorname = 'American Express Consumer Affiliate' then 'AMEX'
    when rev.vendorname = 'Bank of America - Credit Cards' then 'BANK_OF_AMERICA'
    when rev.vendorname = 'Barclaycard' then 'BARCLAYCARD'
    when rev.vendorname = 'Capital Bank' then 'CAPITAL_BANK'
    when rev.vendorname = 'Capital One Auto Finance' then 'CAPITAL_ONE'
    when rev.vendorname = 'Capital One Credit Cards' then 'CAPITAL_ONE'
    when rev.vendorname = 'Chase' then 'CHASE'
    when rev.vendorname = 'Citi Credit Cards' then 'CITI_CREDIT_CARDS'
    when rev.vendorname = 'Credit One Bank' then 'CREDIT_ONE'
    when rev.vendorname ilike 'Discover%' then 'DISCOVER'
    when rev.vendorname = 'Fifth Third Bank' then 'FIFTH_THIRD_BANK'
    when rev.vendorname = 'First Progress' then 'FIRST_PROGRESS'
    when rev.vendorname = 'Genesis Bankcard Services' then 'GENESIS_BANKCARD_SERVICES'
    when rev.vendorname = 'JP Morgan Chase (Credit Cards)' then 'CHASE'
    when rev.vendorname = 'Mid America Bank  Trust Company' then 'MID_AMERICA_BANK_TRUST_COMPANY'
    when rev.vendorname = 'Mid America Bank & Trust' then 'MID_AMERICA_BANK_TRUST'
    when rev.vendorname = 'The Bank of Missouri' then 'BK OF MO'
    when rev.vendorname = 'Wells Fargo' then 'WELLS_FARGO'
    when rev.vendorname = 'chase' then 'CHASE'
    when rev.vendorname = 'elevate' then 'ELEVATE'
    when rev.vendorname = 'lcard' then 'LCARD'
    when rev.vendorname ilike 'lendingclub%' then 'LENDINGCLUB'
    when rev.vendorname = 'lendingpoint' then 'LENDINGPOINT'
    when rev.vendorname ilike 'lendingtree%' then 'LENDINGTREE'
    when rev.vendorname = 'lendup' then 'LENDUP'
    when rev.vendorname = 'marcus' then 'MARCUS'
    when rev.vendorname = 'nationaldebtrelief' then 'NATIONALDEBTRELIEF'
    when rev.vendorname = 'netcredit' then 'NETCREDIT'
    when rev.vendorname = 'onemain' then 'ONEMAIN'
    when rev.vendorname = 'opploans' then 'OPPLOANS'
    when rev.vendorname = 'pacificcreditgroup' then 'PACIFIC_CREDIT_GROUP'
    when rev.vendorname ilike 'prosper%' then 'PROSPER'
    when rev.vendorname ilike 'quinstreet%' then 'QUINSTREET'
    when rev.vendorname = 'total_attorney' then 'TOTAL_ATTORNEY'
    when rev.vendorname = 'upstart' then 'UPSTART'
    when rev.vendorname ilike '%U.S%BANK%' then 'US BANK'
    when rev.vendorname = 'lexingtonlaw' then 'LEXINGTON_LAW'
    else upper(rev.vendorname)
    end as partner_rev
from revenue.all_partners_revenue_data rev
left join public.click_applies_properties cap
on cap.clickid = rev.clickid
where 1
and rev.amountreported > 0
and rev.verticalname ilike 'Credit%Cards'
group by 1,2,3

) eventcat
where usertoken = ut 
and dateopened>= createdate
and dateopened<= createdate+7
;



SELECT
 count(distinct usertoken) as cnt
FROM one_offs.nz_potential


# refresh after 1 month
select 
count(distinct usertoken)
from one_offs.nz_potential
where (last_trade_line_dt - application_date) >=30



SELECT
cohort
, count(distinct usertoken) as cnt
FROM one_offs.nz_potential
where creditor is not null
group by 1




select 
cohort
, count(distinct usertoken) as cnt
from one_offs.nz_potential
where rev_partner = creditor
and creditor is not null
group by 1




SELECT
partner
, creditor
, count(distinct usertoken) as cnt
FROM one_offs.nz_potential
where creditor is not null
group by 1,2
order by 3 desc



SELECT
card_name
, creditor
, count(distinct usertoken) as cnt
FROM one_offs.nz_potential
where creditor is not null
group by 1,2
order by 3 desc


SELECT
cohort
, count(distinct usertoken) as cnt
FROM one_offs.nz_potential
where partner = 'CAPITAL_ONE'
and creditor = 'CAPITAL_ONE'
group by 1



select card_name
,count(distinct usertoken)
from one_offs.nz_potential
where partner ilike  'Capital One Credit Cards%'
group by 1



SELECT
*
FROM one_offs.nz_potential
where partner = 'CAPITAL_ONE'
and creditor = 'CAPITAL_ONE'
limit 100



SELECT
(dateopened - application_date) as dt
, count(distinct usertoken) as cnt
FROM one_offs.nz_potential
where partner = 'CAPITAL_ONE'
and creditor = 'CAPITAL_ONE'
and (dateopened - application_date)<30
group by 1
