select
x.months_since
, x.prv
, x.cur
, cast(round(y.average_vantage3_change,2) as decimal(5,2)) as open_change
, cast(round(x.average_vantage3_change,2) as decimal(5,2))  as mixed_change
, y.user_cnt as open_cnt
, x.user_cnt as mixed_cnt

from
	(SELECT
	floor(months_between((DATE(next.reportpulldate)), (DATE(pivot.reportpulldate ))))  AS months_since
	,(pivot.open_worst_late_count + pivot.closed_late_count) as prv
	,(next.open_worst_late_count + next.closed_late_count ) as cur
	,AVG(next.vantage3::float - pivot.vantage3::float) AS average_vantage3_change
	, ntile(20) over(order by (next.vantage3::float - pivot.vantage3::float) asc)

	, COUNT(DISTINCT pivot.usertoken) as user_cnt
	FROM one_offs.rb_pivot  AS pivot
	INNER JOIN one_offs.rb_pivot  AS next
	ON next.usertoken = pivot.usertoken
	AND (DATE(pivot.reportpulldate )) < (DATE(next.reportpulldate ))
	AND floor(months_between((DATE(next.reportpulldate)), (DATE(pivot.reportpulldate )))) <= 12
	AND (pivot.open_worst_late_count + pivot.closed_late_count) > (next.open_worst_late_count + next.closed_late_count )

	GROUP BY 1,2,3
	) x



left join
(SELECT
floor(months_between((DATE(next.reportpulldate)), (DATE(pivot.reportpulldate ))))  AS months_since
,pivot.open_worst_late_count as prv
,next.open_worst_late_count as cur
,AVG(next.vantage3::float - pivot.vantage3::float) AS average_vantage3_change
, COUNT(DISTINCT pivot.usertoken) as user_cnt
FROM one_offs.rb_pivot  AS pivot
-- Self join
INNER JOIN one_offs.rb_pivot  AS next
ON next.usertoken = pivot.usertoken
AND (DATE(pivot.reportpulldate )) < (DATE(next.reportpulldate ))
AND floor(months_between((DATE(next.reportpulldate)), (DATE(pivot.reportpulldate )))) <= 12
AND pivot.open_worst_late_count > next.open_worst_late_count

GROUP BY 1,2,3) y
on x.months_since = y.months_since
and x.prv = y.prv
and x.cur = y.cur
ORDER BY 1,2,3





--- COLLECTIONS
select
x.months_since
, x.prv
, x.cur
, cast(round(y.average_vantage3_change,2) as decimal(5,2)) as open_change
, cast(round(x.average_vantage3_change,2) as decimal(5,2))  as mixed_change
, y.user_cnt as open_cnt
, x.user_cnt as mixed_cnt

from
(SELECT
floor(months_between((DATE(next.reportpulldate)), (DATE(pivot.reportpulldate ))))  AS months_since
,pivot.collection_count as prv
,next.collection_count as cur
,AVG(next.vantage3::float - pivot.vantage3::float) AS average_vantage3_change
,COUNT(DISTINCT pivot.usertoken) as user_cnt
FROM one_offs.rb_pivot  AS pivot
INNER JOIN one_offs.rb_pivot  AS next
ON next.usertoken = pivot.usertoken
AND (DATE(pivot.reportpulldate )) < (DATE(next.reportpulldate ))
AND floor(months_between((DATE(next.reportpulldate)), (DATE(pivot.reportpulldate )))) <= 12
AND pivot.collection_count > next.collection_count

GROUP BY 1,2,3) x
left join
(SELECT
floor(months_between((DATE(next.reportpulldate)), (DATE(pivot.reportpulldate ))))  AS months_since
,pivot.collection_open_count as prv
,next.collection_open_count as cur
,AVG(next.vantage3::float - pivot.vantage3::float) AS average_vantage3_change
, COUNT(DISTINCT pivot.usertoken) as user_cnt
FROM one_offs.rb_pivot  AS pivot
-- Self join
INNER JOIN one_offs.rb_pivot  AS next
ON next.usertoken = pivot.usertoken
AND (DATE(pivot.reportpulldate )) < (DATE(next.reportpulldate ))
AND floor(months_between((DATE(next.reportpulldate)), (DATE(pivot.reportpulldate )))) <= 12
AND pivot.collection_open_count > next.collection_open_count

GROUP BY 1,2,3) y
on x.months_since = y.months_since
and x.prv = y.prv
and x.cur = y.cur
ORDER BY 1,2,3
limit 100






--- any negative marks
select
x.months_since
, x.prv
, x.cur
, cast(round(y.average_vantage3_change,2) as decimal(5,2)) as open_change
, cast(round(x.average_vantage3_change,2) as decimal(5,2))  as mixed_change
, y.user_cnt as open_cnt
, x.user_cnt as mixed_cnt

from
(SELECT
floor(months_between((DATE(next.reportpulldate)), (DATE(pivot.reportpulldate ))))  AS months_since

,(pivot.collection_count+ pivot.open_worst_late_count + pivot.closed_late_count) as prv
,(next.collection_count + next.open_worst_late_count + next.closed_late_count) as cur

,AVG(next.vantage3::float - pivot.vantage3::float) AS average_vantage3_change
,COUNT(DISTINCT pivot.usertoken) as user_cnt
FROM one_offs.rb_pivot  AS pivot
INNER JOIN one_offs.rb_pivot  AS next
ON next.usertoken = pivot.usertoken
AND (DATE(pivot.reportpulldate )) < (DATE(next.reportpulldate ))
AND floor(months_between((DATE(next.reportpulldate)), (DATE(pivot.reportpulldate )))) <= 12
AND (pivot.collection_count+ pivot.open_worst_late_count + pivot.closed_late_count)  > (next.collection_count + next.open_worst_late_count + next.closed_late_count)

GROUP BY 1,2,3) x
left join
(SELECT
floor(months_between((DATE(next.reportpulldate)), (DATE(pivot.reportpulldate ))))  AS months_since
,(pivot.collection_open_count + pivot.open_worst_late_count ) as prv
,(next.collection_open_count + next.open_worst_late_count ) as cur
,AVG(next.vantage3::float - pivot.vantage3::float) AS average_vantage3_change
, COUNT(DISTINCT pivot.usertoken) as user_cnt
FROM one_offs.rb_pivot  AS pivot
-- Self join
INNER JOIN one_offs.rb_pivot  AS next
ON next.usertoken = pivot.usertoken
AND (DATE(pivot.reportpulldate )) < (DATE(next.reportpulldate ))
AND floor(months_between((DATE(next.reportpulldate)), (DATE(pivot.reportpulldate )))) <= 12
AND (pivot.collection_open_count +pivot.open_worst_late_count)  > (next.collection_open_count +next.open_worst_late_count )

GROUP BY 1,2,3) y
on x.months_since = y.months_since
and x.prv = y.prv
and x.cur = y.cur
where x.prv <=200
ORDER BY 1,2,3
limit 100







---------- updated request
SELECT
floor(months_between((DATE(next.reportpulldate)), (DATE(pivot.reportpulldate ))))  AS months_since
,(pivot.collection_open_count + pivot.open_worst_late_count ) as prv
,(next.collection_open_count + next.open_worst_late_count ) as cur
,(next.vantage3::float - pivot.vantage3::float) AS score_change
FROM one_offs.rb_pivot  AS pivot
-- Self join
INNER JOIN one_offs.rb_pivot  AS next
ON next.usertoken = pivot.usertoken
AND (DATE(pivot.reportpulldate )) < (DATE(next.reportpulldate ))
AND floor(months_between((DATE(next.reportpulldate)), (DATE(pivot.reportpulldate )))) <= 2
AND (pivot.collection_open_count +pivot.open_worst_late_count)  > (next.collection_open_count +next.open_worst_late_count )
and (next.collection_open_count + next.open_worst_late_count )<=20
and (pivot.collection_open_count + pivot.open_worst_late_count )<=20




SELECT
floor(months_between((DATE(next.reportpulldate)), (DATE(pivot.reportpulldate ))))  AS months_since
,pivot.open_worst_late_count as prv
,next.open_worst_late_count as cur
,(next.vantage3::float - pivot.vantage3::float) AS score_change
FROM one_offs.rb_pivot  AS pivot
-- Self join
INNER JOIN one_offs.rb_pivot  AS next
ON next.usertoken = pivot.usertoken
AND (DATE(pivot.reportpulldate )) < (DATE(next.reportpulldate ))
AND floor(months_between((DATE(next.reportpulldate)), (DATE(pivot.reportpulldate )))) <= 2
AND pivot.open_worst_late_count  > next.open_worst_late_count 
and (next.collection_open_count + next.open_worst_late_count )<=20
and (pivot.collection_open_count + pivot.open_worst_late_count )<=20



SELECT
floor(months_between((DATE(next.reportpulldate)), (DATE(pivot.reportpulldate ))))  AS months_since
, pivot.collection_open_count as prv
, next.collection_open_count as cur
, (next.vantage3::float - pivot.vantage3::float) AS score_change
FROM one_offs.rb_pivot  AS pivot
-- Self join
INNER JOIN one_offs.rb_pivot  AS next
ON next.usertoken = pivot.usertoken
AND (DATE(pivot.reportpulldate )) < (DATE(next.reportpulldate ))
AND floor(months_between((DATE(next.reportpulldate)), (DATE(pivot.reportpulldate)))) <= 2
AND (pivot.collection_open_count) > (next.collection_open_count)
and (next.collection_open_count + next.open_worst_late_count )<=20
and (pivot.collection_open_count + pivot.open_worst_late_count )<=20




