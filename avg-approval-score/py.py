
# coding: utf-8

# In[ ]:


import matplotlib.pyplot as plt
import numpy as np
import util as util
import datetime
import pandas as pd
import matplotlib.ticker as mtick


# In[ ]:


list_partner = ['1007'
,'31376'
,'31377'
,'31402'
,'13351969'
,'22966258'
,'60700002'
,'60700003'
,'60700004'
,'229610087'
,'234110088'
,'240910629'
,'1e82abb5adad42668d0e751a2a2b3a24'
,'5d803c89206740a0af6a3c7ff26cce60'
,'AmexBCP'
,'c06a9a6e87224c45988ea1ce6847c2d8'
,'c59398cf858b4a97b5e7fa5975b91ee2'
,'ce293c61587c41c29a64c5685cb78e40'
,'ChaseFreedom'
,'WellsFargo'
,'13216092'
,'13216096'
,'5a7a8298f7b04c1ab65391a3dc5130b3']


# In[ ]:


for item in list_partner:
    query = """
    drop table if exists one_offs.journey_click_applies_properties;
    create table one_offs.journey_click_applies_properties
    DISTKEY (usertoken)
    as
    (
    SELECT clickid, usertoken, createdate, creditinfoid
    FROM click_applies_properties
    WHERE product_offer = '""" + item+ """'
    AND createdate >= current_date -180
    );

    drop table if exists one_offs.journey_click_applies_properties_vantage3;
    create table one_offs.journey_click_applies_properties_vantage3
    DISTKEY (clickid)
    as
    (
    SELECT clickid, JCAP.usertoken, JCAP.createdate, vantage3
    FROM one_offs.journey_click_applies_properties JCAP
    LEFT JOIN credit_report_profiles CRP
    ON JCAP.creditinfoid = CRP.creditinfoid
    WHERE CRP.creditinfoid IS NOT NULL
    UNION ALL
    SELECT clickid, usertoken, createdate, vantage3
    FROM (
        SELECT U.*, vantage3
        , ROW_NUMBER() OVER(PARTITION BY CRP.usertoken ORDER BY reportpulldate DESC) AS rn
        FROM credit_report_profiles CRP
        INNER JOIN (
        SELECT JCAP.*
        FROM one_offs.journey_click_applies_properties JCAP
        LEFT JOIN credit_report_profiles CRP
        ON JCAP.creditinfoid = CRP.creditinfoid
        WHERE CRP.creditinfoid IS NULL
        AND (JCAP.creditinfoid IS NULL
        OR JCAP.creditinfoid = 0)
    ) U
    ON CRP.usertoken = U.usertoken
    WHERE reportpulldate <= U.createdate
    AND DATEDIFF(DAY, reportpulldate, U.createdate) <= 30
    )
    WHERE rn = 1
    );

    SELECT JCAPV.*
    FROM revenue.approvals_declines_credit_cards ADCC
    INNER JOIN one_offs.journey_click_applies_properties_vantage3 JCAPV
    ON ADCC.clickid = JCAPV.clickid
    WHERE approved = 'true'

    """
    df = util.redshift_query_to_df(query)
    x= df.vantage3.values
    try:
        print [item, df.shape[0], np.percentile(x, 5, axis=0), np.percentile(x, 20, axis=0), np.percentile(x, 40, axis=0)
        ,np.percentile(x, 50, axis=0), np.percentile(x, 60, axis=0), np.mean(x)]
    except IndexError:
        print item, x

