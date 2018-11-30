select creditlimit
, count(distinct usertoken)
from public.credit_report_tradelines
where accounttype ilike '%Secured credit card%'
and (creditor ilike '%CAP%ONE%'
OR creditor ilike '%CAP1%'
OR creditor ilike '%indigo%' )
and creditor not ilike '%capstone%'
group by 1
order by 2 desc
limit 100





select *
from public.credit_report_tradelines
where accounttype ilike '%Secured credit card%'
and (creditor ilike '%CAP%ONE%'
OR creditor ilike '%CAP1%'
OR creditor ilike '%indigo%' )
and creditor not ilike '%capstone%'
group by 1
order by 2 desc
limit 100






SELECT
percentile.months_since_last_pull
, percentile.credit_score_band
, percentile.current_cc_count
, percentile.current_collection_count
, average_vantage3_change
, percentile_20
, percentile_80
, distinct_users
FROM
(
		SELECT
		floor(months_between((DATE(next.reportpulldate )), (DATE(curr.reportpulldate ))))  AS months_since_last_pull
		,curr.vantage3 - (curr.vantage3 % 50)  AS credit_score_band
		,CASE WHEN curr.cc_count<=3 THEN curr.cc_count
		WHEN curr.cc_count>3 THEN 4
		else null
		end as current_cc_count
		,CASE WHEN curr.collection_count<=1 THEN curr.collection_count
		WHEN curr.collection_count>1 THEN 2
		else null
		end as current_collection_count
		,AVG(next.vantage3::float - curr.vantage3::float ) AS average_vantage3_change
		,PERCENTILE_CONT(0.2) WITHIN GROUP (ORDER BY next.vantage3::float - curr.vantage3::float) AS percentile_20
		,PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY next.vantage3::float - curr.vantage3::float) AS percentile_80
	FROM one_offs.rb_pivot  AS curr
	INNER JOIN one_offs.rb_pivot  AS next
	ON next.usertoken = curr.usertoken
	and (DATE(curr.reportpulldate )) < (DATE(next.reportpulldate ))
	WHERE (floor(months_between((DATE(next.reportpulldate )), (DATE(curr.reportpulldate ))))  >= 1
	AND floor(months_between((DATE(next.reportpulldate )), (DATE(curr.reportpulldate ))))  <= 11)
	AND curr.totalaccounts = (next.totalaccounts - 1)
	AND next.delinquentaccounts  <= curr.delinquentaccounts
	AND next.derogatoryaccounts  <= curr.derogatoryaccounts
	AND (next.cc_count - curr.cc_count ) = 1
	AND (next.total_limit  - curr.total_limit ) = 300
	GROUP BY 1,2,3,4
	ORDER BY 1,2,3,4) percentile
inner join
(
	SELECT
		floor(months_between((DATE(next.reportpulldate )), (DATE(curr.reportpulldate ))))  AS months_since_last_pull
		,curr.vantage3 - (curr.vantage3 % 50)  AS credit_score_band
		,CASE WHEN curr.cc_count<=3 THEN curr.cc_count
		else 4
		end as current_cc_count
		,CASE WHEN curr.collection_count<=1 THEN curr.collection_count
		else 2
		end as current_collection_count
		,COUNT(DISTINCT curr.usertoken ) AS distinct_users

	FROM one_offs.rb_pivot  AS curr
	INNER JOIN one_offs.rb_pivot  AS next
	ON next.usertoken = curr.usertoken
	and (DATE(curr.reportpulldate )) < (DATE(next.reportpulldate ))

	WHERE (floor(months_between((DATE(next.reportpulldate )), (DATE(curr.reportpulldate ))))  >= 1
	AND floor(months_between((DATE(next.reportpulldate )), (DATE(curr.reportpulldate ))))  <= 11)
	AND curr.totalaccounts = (next.totalaccounts - 1)
	AND next.delinquentaccounts  <= curr.delinquentaccounts
	AND next.derogatoryaccounts  <= curr.derogatoryaccounts
	AND (next.cc_count - curr.cc_count ) = 1
	AND (next.total_limit  - curr.total_limit ) = 300
	GROUP BY 1,2,3,4
	ORDER BY 1,2,3,4
) cnt
on percentile.months_since_last_pull = cnt.months_since_last_pull
and percentile.credit_score_band = cnt.credit_score_band
and percentile.current_collection_count = cnt.current_collection_count
and percentile.current_cc_count = cnt.current_cc_count


limit 100













---- combining 300-450 together





SELECT
percentile.months_since_last_pull
, percentile.current_cc_count
, percentile.current_collection_count
, average_vantage3_change
, percentile_20
, percentile_80
, distinct_users
FROM
(
		SELECT
		floor(months_between((DATE(next.reportpulldate )), (DATE(curr.reportpulldate ))))  AS months_since_last_pull
		,CASE WHEN curr.cc_count<=3 THEN curr.cc_count
		WHEN curr.cc_count>3 THEN 4
		else null
		end as current_cc_count
		,CASE WHEN curr.collection_count<=1 THEN curr.collection_count
		WHEN curr.collection_count>1 THEN 2
		else null
		end as current_collection_count
		,AVG(next.vantage3::float - curr.vantage3::float ) AS average_vantage3_change
		,PERCENTILE_CONT(0.2) WITHIN GROUP (ORDER BY next.vantage3::float - curr.vantage3::float) AS percentile_20
		,PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY next.vantage3::float - curr.vantage3::float) AS percentile_80
	FROM one_offs.rb_pivot  AS curr
	INNER JOIN one_offs.rb_pivot  AS next
	ON next.usertoken = curr.usertoken
	and (DATE(curr.reportpulldate )) < (DATE(next.reportpulldate ))
	WHERE (floor(months_between((DATE(next.reportpulldate )), (DATE(curr.reportpulldate ))))  >= 1
	AND floor(months_between((DATE(next.reportpulldate )), (DATE(curr.reportpulldate ))))  <= 11)
	AND curr.totalaccounts = (next.totalaccounts - 1)
	AND next.delinquentaccounts  <= curr.delinquentaccounts
	AND next.derogatoryaccounts  <= curr.derogatoryaccounts
	AND (next.cc_count - curr.cc_count ) = 1
	AND (next.total_limit  - curr.total_limit ) = 300
	AND curr.vantage3 <= 499
	AND curr.vantage3 >= 300


	GROUP BY 1,2,3
	ORDER BY 1,2,3) percentile
inner join
(
	SELECT
		floor(months_between((DATE(next.reportpulldate )), (DATE(curr.reportpulldate ))))  AS months_since_last_pull
		,CASE WHEN curr.cc_count<=3 THEN curr.cc_count
		else 4
		end as current_cc_count
		,CASE WHEN curr.collection_count<=1 THEN curr.collection_count
		else 2
		end as current_collection_count
		,COUNT(DISTINCT curr.usertoken ) AS distinct_users

	FROM one_offs.rb_pivot  AS curr
	INNER JOIN one_offs.rb_pivot  AS next
	ON next.usertoken = curr.usertoken
	and (DATE(curr.reportpulldate )) < (DATE(next.reportpulldate ))

	WHERE (floor(months_between((DATE(next.reportpulldate )), (DATE(curr.reportpulldate ))))  >= 1
	AND floor(months_between((DATE(next.reportpulldate )), (DATE(curr.reportpulldate ))))  <= 11)
	AND curr.totalaccounts = (next.totalaccounts - 1)
	AND next.delinquentaccounts  <= curr.delinquentaccounts
	AND next.derogatoryaccounts  <= curr.derogatoryaccounts
	AND (next.cc_count - curr.cc_count ) = 1
	AND (next.total_limit  - curr.total_limit ) = 300
	AND curr.vantage3 <= 499
	AND curr.vantage3 >= 300
	GROUP BY 1,2,3
	ORDER BY 1,2,3
) cnt
on percentile.months_since_last_pull = cnt.months_since_last_pull
and percentile.current_collection_count = cnt.current_collection_count
and percentile.current_cc_count = cnt.current_cc_count


limit 100

























