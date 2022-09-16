from pyspark.sql import SparkSession,functions


spark = SparkSession.builder.master("local").appName("mysqlToS3").getOrCreate()

spark.sparkContext._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", \
                                     "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")

data="s3a://practisekesava/folder1/rating1.csv"

df_s3=spark.read.format('csv').option('header','true').option('inferSchema','true').load(data)
#df_s3=spark.read.format('csv').option('header','true').option('inferSchema','true').load("C:\\Users\\Saikesava Rakonda\\Desktop\\Pyspark Practise\\Source Files\\rating1.csv")
print('read from S3 was successfull')

df_s3=df_s3.withColumn('DayWid',functions.current_date())

df_s3.show()
df_s3.write.format('jdbc').\
option('url','jdbc:mysql://database1.cj8umwyfacsg.ap-south-1.rds.amazonaws.com:3306').\
option('driver','com.mysql.jdbc.Driver').\
option('user','root').\
option('password','kesava123').\
option('dbtable','classicmodels.day_rating').\
mode('append').\
save()

print('Table loaded with data successfully')