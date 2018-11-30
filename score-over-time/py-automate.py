
# coding: utf-8

# In[ ]:


import matplotlib.pyplot as plt
import numpy as np
import util as util
import datetime
import matplotlib.ticker as mtick
import pandas as pd


# In[ ]:


query = """
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
"""
df = util.redshift_query_to_df(query)


# In[ ]:


cs = list(set(df.credit_score_band))
cc_count = list(set(df.current_cc_count))
cc_count.sort()

cs.sort()
cs = cs[:-1]


# In[ ]:


i = 3
j = 2

tmp = df[df.credit_score_band == cs[i]]
tmp = tmp[[u'months_since_last_pull', u'current_cc_count',
       u'current_collection_count', u'average_vantage3_change',
       u'percentile_20', u'percentile_80', u'distinct_users']]

tmp = tmp[tmp.current_cc_count == cc_count[j]]
tmp = tmp[[u'months_since_last_pull', u'current_collection_count', u'average_vantage3_change',
       u'percentile_20', u'percentile_80', u'distinct_users']]

print 'credit card count',cc_count[j], 'credit score' ,cs[i]

tmp = pd.pivot_table(tmp,index=['months_since_last_pull'],columns=['current_collection_count']).reset_index()
tmp1 = pd.DataFrame()
for i in range(3):
    tmp1['cnt'+str(i)] = tmp.distinct_users[i]
print tmp1

tmp1 = pd.DataFrame()
for i in range(3):
    tmp1['avg_'+str(i)] = tmp.average_vantage3_change[i]
    tmp1['p20_'+str(i)] = tmp.percentile_20[i]
    tmp1['p80_'+str(i)] = tmp.percentile_80[i]


# In[289]:


query = """
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
"""
df = util.redshift_query_to_df(query)


# In[307]:


j=0
tmp = df[[u'months_since_last_pull', u'current_cc_count',
       u'current_collection_count', u'average_vantage3_change',
       u'percentile_20', u'percentile_80', u'distinct_users']]

tmp = tmp[tmp.current_cc_count == cc_count[j]]
tmp = tmp[[u'months_since_last_pull', u'current_collection_count', u'average_vantage3_change',
       u'percentile_20', u'percentile_80', u'distinct_users']]

print 'credit card count',cc_count[j]

tmp = pd.pivot_table(tmp,index=['months_since_last_pull'],columns=['current_collection_count']).reset_index()
tmp1 = pd.DataFrame()
for i in range(3):
    tmp1['cnt'+str(i)] = tmp.distinct_users[i]
print tmp1

tmp1 = pd.DataFrame()
for i in range(3):
    tmp1['avg_'+str(i)] = tmp.average_vantage3_change[i]
    tmp1['p20_'+str(i)] = tmp.percentile_20[i]
    tmp1['p80_'+str(i)] = tmp.percentile_80[i]


# In[308]:


tmp1

