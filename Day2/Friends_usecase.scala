// Databricks notebook source
//FileStore/tables/FriendsData.csv

// COMMAND ----------

val data=sc.textFile("/FileStore/tables/FriendsData.csv")
data.take(2)

// COMMAND ----------

val noheaderRdd=data.filter(x => !x.contains("name"))
noheaderRdd.take(2)

// COMMAND ----------

// DBTITLE 1,Avg_age
val ageRdd=noheaderRdd.map(x=>(x.split(",")(2).toInt,(1,x.split(",")(3).toInt)))
//ageRdd.collect()
val Age=ageRdd.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
Age.collect()

// COMMAND ----------

for((age,avg_friends)<- finalRdd.collect()) println(age+":"+avg_friends)

// COMMAND ----------

// DBTITLE 1,Max_age
val ageRdd1=noheaderRdd.map(x=>(x.split(",")(2).toInt,x.split(",")(3).toInt)) 
val finalRdd=ageRdd1.reduceByKey(Math.max(_,_))
finalRdd.collect()

