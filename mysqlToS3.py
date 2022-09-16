from pyspark.sql import *

spark = SparkSession.builder.master("local").appName("mysqlToS3").getOrCreate()

df_sql=spark.read.format('jdbc').\
option('url','jdbc:mysql://database1.cj8umwyfacsg.ap-south-1.rds.amazonaws.com:3306').\
option('driver','com.mysql.jdbc.Driver').\
option('user','root').\
option('password','kesava123').\
option('dbtable','classicmodels.customers').\
load()

df_sql.show()

spark.sparkContext._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", \
                                     "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.fast.upload",'true')
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.fast.upload.buffer", "bytebuffer")

data="s3a://practisekesava/folder1/customers"

df_sql.write.format('csv').option('header','true').save(data,mode='overwrite')

print('Write to S3 was successfull')