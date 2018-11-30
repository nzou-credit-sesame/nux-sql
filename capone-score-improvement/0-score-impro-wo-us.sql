with clicks as
(
	select
		usertoken
		, dateopened  as clickdate
		, creditinfoid
		, paystatushistory
		, currentbalance
		, creditlimit

	from credit_report_tradelines
	where 1
	and accounttype ilike 'credit%'
	and (creditor ilike '%cap%1' or creditor ilike 'cap%one%')
	and creditor not ilike 'CAPSTONE%'
	and openclosed ilike 'open'
	and dateopened >= current_date - 365
	and (datereported - dateopened)<=62
	and creditlimit between 300 and 1000
),

first_cs as
(
	select x.*
	from
	(
		select
			crt.usertoken
			, crt.clickdate
			, cph.creditinfodate as initial_date
			, cph.vantage3 as initial_cs
			, row_number() over(PARTITION BY crt.usertoken order by cph.creditinfodate desc) as rn
		from clicks crt
		inner Join public.credit_profile_history cph
		ON cph.usertoken=crt.usertoken
		and date(cph.creditinfodate) <= clickdate
		and date(cph.creditinfodate) >= clickdate-10
	) x
	where rn = 1
),

second_cs as
(
	select x.*
	from
	(
		select
			crt.*
			, cph.creditinfodate as new_date
			, cph.vantage3 as new_cs
			, row_number() over(PARTITION BY crt.usertoken order by cph.creditinfodate asc) as rn
		from clicks crt
		inner Join public.credit_profile_history cph
		ON cph.usertoken=crt.usertoken
		and cph.creditinfoid = crt.creditinfoid
		and date(cph.creditinfodate) <= clickdate + 60
		and date(cph.creditinfodate) >= clickdate + 30
		) x
	where rn = 1
),

profile as (
	select f.usertoken
	, f.initial_date
	, s.clickdate
	, f.initial_cs
	, s.new_cs
	, s.new_date
	, paystatushistory
	, currentbalance
	, creditlimit
	from first_cs f
	inner join second_cs s
	on f.usertoken = s.usertoken
	and f.clickdate = s.clickdate
)

select
usertoken
,(new_cs - initial_cs) as score_change
, case when paystatushistory is null then 0
else 1
end as payment
,(cast(currentbalance as decimal(18,5))/creditlimit) as cu

from profile
where 1
and new_date > initial_date





select
case when paystatushistory is null then 0
else 1
end as payment
,avg((cast(currentbalance as decimal(18,5))/creditlimit))

from profile
where 1
and new_date > initial_date
group by 1
