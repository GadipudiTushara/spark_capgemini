// Databricks notebook source
//FileStore/tables/Property_data.csv
val data=sc.textFile("/FileStore/tables/Property_data.csv")

// COMMAND ----------

val removedheader=data.filter(x => !x.contains("Price"))

// COMMAND ----------

val roomRdd=removedheader.map(x=>(x.split(",")(3).toInt,(1,x.split(",")(2).toDouble)))
roomRdd.map(x=>x._2._2).take(10)
roomRdd.count()

// COMMAND ----------

//roomRdd.map(x=>x._2._2).collect()
val reducedRdd=roomRdd.reduceByKey((x,y)=>(x._1+y._1, x._2+y._2))

// COMMAND ----------

val finalRdd=reducedRdd.mapValues(x=>x._2/ x._1)
finalRdd.saveAsTextFile("propertyfinal.csv")

// COMMAND ----------

for((bedroom, avg) <- finalRdd.collect()) println(bedroom+":"+avg)
