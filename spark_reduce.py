##### Examples of how to use functools.reduce with pyspark to streamline analysis on large datasets

from datetime import datetime
import datetime as dt
from functools import reduce

###### STACK MULTIPLE DF'S ######
# suppose you have a list of dataframes all with the same structure that need to be systematically appened
# to stack a pyspark dataframe, you can use the .union or .unionAll method
# to repeat this, create a list of dataframes and use functools' reduce

paths = [... 'list of dataframe paths' ...]
dfs = [spark.read.parquet(l).select(...) for l in paths]
combined_df = reduce(DataFrame.unionAll, dfs)


###### NESTED 'OR' ######
# say you want to query members active in this time period:
start, end = datetime(2020,7,1,0,0), datetime(2021,6,30,0,0)

# if you have columns for each month (cov_1801 for coverage Jan 2018, for example) 
# and want to find any member with activity in any month in your date range
# you can use nested ors
# to facilitate this, use pyspark's filter with functools' reduce

#first create the columns
l = []
val = start
while val < end :
  l.append(val) 
  val = (val + dt.timedelta(days=31)).replace(day=1)
#list of column names (cov_2007, cov_2008, ..., cov_2106)
cov = [f"cov_{repr(x.year - 2000)}{repr(x.month).zfill(2)}" for x in l]
#list of pyspark filters
flt_cov = [col(c) == 1 for c in cov]

# here is the nested or function we will use in reduce, a simple lambda function
nestor = lambda u, v : (u) | (v)

person_df = (spark.read.parquet('file path')
             .filter(reduce(nestor, flt_cov))
             .select(...)
            )




