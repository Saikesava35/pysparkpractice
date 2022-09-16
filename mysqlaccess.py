from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").appName("mysqlaccess").getOrCreate()

df_sql=spark.read.format('jdbc').\
option('url','jdbc:mysql://database1.cj8umwyfacsg.ap-south-1.rds.amazonaws.com:3306').\
option('driver','com.mysql.jdbc.Driver').\
option('user','root').\
option('password','kesava123').\
option('dbtable','classicmodels.customers').\
load()


print(df_sql.show())