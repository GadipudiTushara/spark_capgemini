// Databricks notebook source
val data = sc.textFile("/FileStore/tables/airports.text")

// COMMAND ----------

val dataRdd=data.map(x=>(x.split(",")(1),x.split(",")(3)))
dataRdd.take(5)

// COMMAND ----------

val data_except_canada=dataRdd.filter(x=>x._2!="\"Canada\"")
data_except_canada.take(10)

// COMMAND ----------

val ListData=List("Tushara 2020","Nancy 1998","Abhinav 1997","Karthik 2100")


// COMMAND ----------

val ListRdd=sc.parallelize(ListData)

// COMMAND ----------

val key_val=ListRdd.map(x=>(x.split(" ")(0),x.split(" ")(1).toInt))
key_val.mapValues(x=>x+10).collect()

// COMMAND ----------

val Air_key=data.map(x=>(x.split(",")(1),x.split(",")(11).toLowerCase))
Air_key.collect()

// COMMAND ----------

 
