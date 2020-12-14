// Databricks notebook source
//FileStore/tables/FriendsData.csv

// COMMAND ----------

val data=sc.textFile("/FileStore/tables/FriendsData.csv")
data.take(2)

// COMMAND ----------

val noheaderRdd=data.filter(x => !x.contains("name"))
noheaderRdd.take(2)

// COMMAND ----------

val ageRdd=noheaderRdd.map(x=>(x.split(",")(2).toInt,(1,x.split(",")(3).toInt)))
val Age=ageRdd.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
Age.collect()

// COMMAND ----------

val finalRdd=Age.mapValues(x => max(x._2))
//for((age,avg_friends)<- finalRdd.collect()) println(age+":"+avg_friends)
