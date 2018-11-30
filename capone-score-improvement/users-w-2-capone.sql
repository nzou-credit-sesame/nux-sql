with temp1 as
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
	--and accounttype ilike 'credit%'
	and (creditor ilike '%cap%1' or creditor ilike 'cap%one%')
	and creditor not ilike 'CAPSTONE%'
	and designation = 'Individual'
	and openclosed ilike 'open'
	and creditinfodate>= '2017-01-01'
		),

temp2 as (
	select
	usertoken
	, creditinfodate
	from temp1
	where 1
	and rn >=2
	group by 1,2
),

temp3 as (
	select
		t1.*
	from temp1 t1
	inner join temp2 t2
	using (usertoken)
	where t1.creditinfodate >= t2.creditinfodate
	group by 1,2,3,4,5,6,7

)

select * from temp3
limit 10
