#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark import SparkContext
from sklearn.linear_model import LinearRegression
import numpy as np
import re
from operator import add
import csv
import io
import sys


def do_csv(file):
    output = io.StringIO("")
    csv.writer(output).writerow(file)
    return output.getvalue().strip()

def violation(partId, records):
    if partId==0:
        next(records)
    import csv
    reader = csv.reader(records)
    for row in reader:
        if (row[23].isupper()==False) and  (row[23]!='') and (row[24]!='') and ((re.sub('[^0-9-]', '', row[23])).split("-")[-1]!='') and ((re.sub('[^0-9-]', '', row[23])).split("-")[0]!='') and (row[21]!='') and (row[4].split("/")[-1] in ["2015", "2016", "2017", "2018", "2019"]):
            if "-" in row[23]:
                house_num=re.sub('[^0-9-]', '', row[23])
                house_num=(int(house_num.split("-")[0]), int(house_num.split("-")[-1]))
            else:
                house_num=re.sub('[^0-9-]', '', row[23])
                house_num=(int(house_num),)
            if row[21] in ["MAN", "MH", "MN", "NEWY", "NEW Y", "NY"]:
                v_county=1
            elif row[21] in ["BRONX", "BX"]:
                v_county=2
            elif row[21] in ["BK", "K", "KING", "KINGS"]:
                v_county=3
            elif row[21] in ["Q", "QN", "QNS", "QU", "QUEEN"]:
                v_county=4
            elif row[21] in ["R", "RICHMOND"]:
                v_county=5
       
            street_name=row[24].lower()
            issue_date=(row[4].split("/")[-1])
            yield (v_county,street_name), (house_num,  issue_date)

def centerline(partId, records):
    if partId==0:
        next(records)
    import csv
    reader = csv.reader(records)
    for row in reader:
        if row[2]!='' and row[3]!='' and row[4]!='' and row[5]!='' and row[10]!='' and row[28]!='' and row[2]!='0' and row[3]!='0' and row[4]!='0' and row[5]!='0':
            if "-" in row[2]:
                l_low_hn=(int(row[2].split("-")[0]),int(row[2].split("-")[-1]))
            else:
                l_low_hn=(int(row[2]),)
                
            if "-" in row[3]:
                l_high_hn=(int(row[3].split("-")[0]),int(row[3].split("-")[-1]))
            else:
                l_high_hn=(int(row[3]),)
            if "-" in row[4]:
                r_low_hn=(int(row[4].split("-")[0]),int(row[4].split("-")[-1]))
            else:
                r_low_hn=(int(row[4]),)
            if "-" in row[5]:
                r_high_hn=(int(row[5].split("-")[0]),int(row[5].split("-")[-1]))
            else:
                r_high_hn=(int(row[5]),)
            
                
            if row[28].lower()==row[10].lower():
        
                yield (int(row[13]), row[28].lower()), (int(row[0]),l_low_hn, l_high_hn, r_low_hn, r_high_hn)
            else:
                yield (int(row[13]), row[28].lower()), (int(row[0]), l_low_hn, l_high_hn, r_low_hn, r_high_hn)
                yield (int(row[13]), row[10].lower()), (int(row[0]), l_low_hn, l_high_hn, r_low_hn, r_high_hn)
                
def put_years(x):
    y2015=0
    y2016=0
    y2017=0
    y2018=0
    y2019=0
    year_count=x[1]
    year=year_count.keys()
    
    if '2015' in year:
        y2015=year_count['2015']
    if '2016' in year:
        y2016=year_count['2016']
    if '2017' in year:
        y2017=year_count['2017']
    if '2018' in year:
        y2018=year_count['2018']
    if '2019' in year:
        y2019=year_count['2019']
        
    return x[0], (y2015, y2016, y2017, y2018, y2019)

def regression(x):
    years_x=np.array((2015, 2016, 2017, 2018, 2019))
    counts_y=np.array((x[1][0], x[1][1], x[1][2], x[1][3],x[1][4]))
    lin_reg=LinearRegression()
    lin_reg.fit(years_x.reshape(-1,1), counts_y)
    ols=round(lin_reg.coef_[0], 2)
    return (x[0], (x[1][0], x[1][1], x[1][2], x[1][3], x[1][4], ols))

def all_ids(partId, records):
    if partId==0:
        next(records)
    import csv
    reader = csv.reader(records)
    for row in reader:
        yield (int(row[0]),(0,0,0,0,0,0))
                
def main(sc1, sc2):
    v1 = sc1.mapPartitionsWithIndex(violation)
    cntr = sc2.mapPartitionsWithIndex(centerline)
    phid = sc2.mapPartitionsWithIndex(all_ids)
    v1.join(cntr).filter(lambda x: ((x[1][0][0][-1]%2==1 and x[1][0][0] <= x[1][1][2] and x[1][0][0] >= x[1][1][1])  or (x[1][0][0][-1]%2==0 and x[1][0][0] <= x[1][1][4] and x[1][0][0] >= x[1][1][3]))).map(lambda x: ((x[1][1][0],x[1][0][1]), 1)).reduceByKey(lambda x,y: x+y).map(lambda x: ((x[0][0]), (x[0][1], x[1]))).groupByKey().mapValues(dict).map(put_years).map(regression).count().saveAsTextFile(sys.argv[1])
    
    
if __name__ == '__main__':
    sc = SparkContext()
    vfiles=sc.textFile("hdfs:///data/share/bdm/nyc_parking_violation/*.csv", use_unicode=True)
    cent = sc.textFile("hdfs:///data/share/bdm/nyc_cscl.csv", use_unicode=True)    
    main(vfiles, cent)
    

