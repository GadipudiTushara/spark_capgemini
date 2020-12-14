// Databricks notebook source
///FileStore/tables/airports.text
val data = List("tush","Gadipudi","tush","hey","Gadipudi","nice")

// COMMAND ----------

val datardd=sc.parallelize(data)
datardd.distinct.count() 

// COMMAND ----------

val data2=List(1,2,3,4,5)
val data2rdd=sc.parallelize(data2)
data2rdd.sum

// COMMAND ----------

val productrdd = data2rdd.reduce((x,y)=>x*y)
productrdd

// COMMAND ----------

///FileStore/tables/primeNumber.text
///FileStore/tables/numberData.csv
val numberrdd=sc.textFile("/FileStore/tables/numberData.csv")
val numrdd=numberrdd.filter(x=>x!="Number")
numrdd.collect()

// COMMAND ----------

// DBTITLE 1,Prime_function
def isprime(x:Int): Boolean = {
  if(x<=1) false
  else if(x==2) true
  else !(2 to (x-1)).exists(i=>x%i==0)
}

// COMMAND ----------

// DBTITLE 1,extracting prime_numbers from numbers data
val primerdd=numrdd.filter(x=>isprime(x.toInt)).map(x=>x.toInt)

// COMMAND ----------

// DBTITLE 1,sum
val summ=primerdd.reduce((x,y)=>x+y)
summ 
