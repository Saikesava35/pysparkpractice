from pyspark.sql import SparkSession,functions
from pyspark.sql.functions import udf, col, when, to_date, length, month, count, round

spark = SparkSession.builder.master("local").appName("mysqlToS3").getOrCreate()

df_policy=spark.read.format('csv').option('header','true').option('inferSchema','true').load("C:\\Users\\Saikesava Rakonda\\Desktop\\Pyspark Practise\\Source Files\\Policy Info.csv")
print('read from local file system was successfull')

'''df_policy.write.format('jdbc').\
option('url','jdbc:mysql://database1.cj8umwyfacsg.ap-south-1.rds.amazonaws.com:3306').\
option('driver','com.mysql.jdbc.Driver').\
option('user','root').\
option('password','kesava123').\
option('dbtable','raw_zone.policyinfo').\
mode('overwrite').\
save()

print('Raw data loaded to respective table')'''

df_policy_gold=df_policy.dropDuplicates(['policy_no']).na.drop()

df_policy_gold=df_policy_gold.withColumn('sex', when(col('sex') == '0', 'female').
                                         when(col('sex') == '1', 'male').
                                         otherwise(col('sex'))
                                         ).\
                                withColumn('smoker', when(col('smoker') == '0', 'no').
                                         when(col('smoker') == '1', 'yes').
                                         otherwise(col('smoker'))
                                         )\
                                .withColumn('policy_date',when(length('policy_date')==8,to_date(col("policy_date"),"MM/dd/yy") ).\
                                                        when(length('policy_date')==10,to_date(col("policy_date"),"MM/dd/yyyy") ))\
                                .withColumn('month_wid',month('policy_date'))
df_policy_gold.show(5)
df_policy_gold.printSchema()

df_policy_gold.cache()
print('Raw data record count: '+ str(df_policy.count()))
print('Cleaned data record count: '+ str(df_policy_gold.count()))


'''df_policy.write.format('jdbc').\
option('url','jdbc:mysql://database1.cj8umwyfacsg.ap-south-1.rds.amazonaws.com:3306').\
option('driver','com.mysql.jdbc.Driver').\
option('user','root').\
option('password','kesava123').\
option('dbtable','gold_zone.policyinfo_cleaned').\
mode('overwrite').\
save()

print('Cleaned data moved to respective table')'''

df_age_policy_smith=df_policy_gold.groupBy('month_wid','age').agg(count('policy_no').alias('no_of_policies'))
df_age_policy_smith.show()

df_sex_policy_smith=df_policy_gold.groupBy('month_wid').pivot('sex').count()
df_sex_policy_smith=df_sex_policy_smith.withColumn('male_per',((col('male')/(col('male')+col('female')))*100).cast('int')).\
    withColumn('female_per',((col('female')/(col('male')+col('female')))*100).cast('int'))
df_sex_policy_smith.show()

df_bmi_policy_smith=df_policy_gold.withColumn('bmi1',round(col('bmi'),1)).groupBy('month_wid','bmi1').agg(count('policy_no').alias('no_of_policies'))
df_bmi_policy_smith.show()

df_child_policy_smith=df_policy_gold.groupBy('month_wid','children').agg(count('policy_no').alias('no_of_policies'))
df_child_policy_smith.show()

df_smoker_policy_smith=df_policy_gold.groupBy('month_wid').pivot('smoker').count()
df_smoker_policy_smith=df_smoker_policy_smith.withColumn('yes_per',((col('yes')/(col('yes')+col('no')))*100).cast('int')).\
    withColumn('no_per',((col('no')/(col('yes')+col('no')))*100).cast('int'))
df_smoker_policy_smith.show()

df_region_policy_smith=df_policy_gold.withColumn('region_updated',when(df_policy_gold.region.startswith('south'),col('region'))\
.otherwise('Other')).groupBy('month_wid').pivot('region_updated').count()
df_region_policy_smith=df_region_policy_smith.withColumn('southeast_per',((col('southeast')/(col('southeast')+col('southwest')+col('Other')))*100).cast('int')).\
withColumn('southwest_per',((col('southwest')/(col('southeast')+col('southwest')+col('Other')))*100).cast('int')).\
withColumn('other_per',((col('Other')/(col('southeast')+col('southwest')+col('Other')))*100).cast('int'))
df_region_policy_smith.show()

df_charges_policy_smith=df_policy_gold.withColumn('charges1',round(col('charges'),1)).groupBy('month_wid','charges1').agg(count('policy_no').alias('no_of_policies'))
df_charges_policy_smith.show()



df_age_policy_smith.write.format('jdbc').\
option('url','jdbc:mysql://database1.cj8umwyfacsg.ap-south-1.rds.amazonaws.com:3306').\
option('driver','com.mysql.jdbc.Driver').\
option('user','root').\
option('password','kesava123').\
option('dbtable','smith_zone.policyinfo_age').\
mode('overwrite').\
save()

df_sex_policy_smith.write.format('jdbc').\
option('url','jdbc:mysql://database1.cj8umwyfacsg.ap-south-1.rds.amazonaws.com:3306').\
option('driver','com.mysql.jdbc.Driver').\
option('user','root').\
option('password','kesava123').\
option('dbtable','smith_zone.policyinfo_sex').\
mode('overwrite').\
save()

df_bmi_policy_smith.write.format('jdbc').\
option('url','jdbc:mysql://database1.cj8umwyfacsg.ap-south-1.rds.amazonaws.com:3306').\
option('driver','com.mysql.jdbc.Driver').\
option('user','root').\
option('password','kesava123').\
option('dbtable','smith_zone.policyinfo_bmi').\
mode('overwrite').\
save()

df_child_policy_smith.write.format('jdbc').\
option('url','jdbc:mysql://database1.cj8umwyfacsg.ap-south-1.rds.amazonaws.com:3306').\
option('driver','com.mysql.jdbc.Driver').\
option('user','root').\
option('password','kesava123').\
option('dbtable','smith_zone.policyinfo_child').\
mode('overwrite').\
save()

df_smoker_policy_smith.write.format('jdbc').\
option('url','jdbc:mysql://database1.cj8umwyfacsg.ap-south-1.rds.amazonaws.com:3306').\
option('driver','com.mysql.jdbc.Driver').\
option('user','root').\
option('password','kesava123').\
option('dbtable','smith_zone.policyinfo_smoker').\
mode('overwrite').\
save()

df_region_policy_smith.write.format('jdbc').\
option('url','jdbc:mysql://database1.cj8umwyfacsg.ap-south-1.rds.amazonaws.com:3306').\
option('driver','com.mysql.jdbc.Driver').\
option('user','root').\
option('password','kesava123').\
option('dbtable','smith_zone.policyinfo_region').\
mode('overwrite').\
save()

df_charges_policy_smith.write.format('jdbc').\
option('url','jdbc:mysql://database1.cj8umwyfacsg.ap-south-1.rds.amazonaws.com:3306').\
option('driver','com.mysql.jdbc.Driver').\
option('user','root').\
option('password','kesava123').\
option('dbtable','smith_zone.policyinfo_charges').\
mode('overwrite').\
save()

print('Table loaded with data successfully')
