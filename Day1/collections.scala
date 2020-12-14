// Databricks notebook source
var new2 = Array(1,2,3,4,5)

// COMMAND ----------

val l:List[Int]=List(12,15,88)

// COMMAND ----------

// DBTITLE 1,sc- spark context - object in databricks which is used to work with cluster
val data = List(1,2,3,4,5)
val creationRDD = sc.parallelize(data)

// COMMAND ----------

// to get result of RDD -> call action on your RDD
creationRDD.collect()

// COMMAND ----------

// get total partitions for your data
creationRDD.partitions.size

// COMMAND ----------

val rddPartition=sc.parallelize(List(1,2,3,4,6),2)

// COMMAND ----------

rddPartition.partitions.size

// COMMAND ----------

rddPartition.count()

// COMMAND ----------

//map -> transformation operations
val newrdd=rddPartition.map( x => x*x*x )
newrdd.collect()
newrdd.take(3)

// COMMAND ----------

newrdd.filter(x=>x%2==0).collect()

// COMMAND ----------

val mainrdd = sc.parallelize(List("hey,girl,you","hello","tushara","G"))
mainrdd.collect()

// COMMAND ----------

mainrdd.map( x => x.split(",")).collect()

// COMMAND ----------

mainrdd.flatMap( x => x.split(",")).collect()

// COMMAND ----------

val rdd0 = sc.parallelize(Array("one","two","three","one","two"))

// COMMAND ----------

val keyrdd=rdd0.map( x => (x,5))
keyrdd.collect()

// COMMAND ----------

keyrdd.reduceByKey(_+_).collect()
