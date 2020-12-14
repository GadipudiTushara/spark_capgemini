// Databricks notebook source
///FileStore/tables/ratings.csv
val data = sc.textFile("/FileStore/tables/ratings.csv")
data.collect()

// COMMAND ----------

val ratingsData=data.map(x=>x.split(",")(2))

ratingsData.collect()

// COMMAND ----------

ratingsData.count()

// COMMAND ----------

ratingsData.countByValue

// COMMAND ----------

///FileStore/tables/airports.text
val airportrdd = sc.textFile("/FileStore/tables/airports.text")



// COMMAND ----------

val usairport=airportrdd.filter(x => x.split(",")(3)=="\"United States\"")

// COMMAND ----------

def splitInput(line:String)={
  val dataSplit = line.split(",")
  //val airportname=(dataSplit(1))
  //val cityname=(dataSplit(2))
  (dataSplit(1),dataSplit(2))
}

// COMMAND ----------

usairport.map(splitInput).take(5)

// COMMAND ----------

usairport.map(line=>{
  val splitData=line.split(",")
  splitData(1)+"\n"+splitData(2)
}).take(5)

// COMMAND ----------

airportrdd.count()


// COMMAND ----------

// DBTITLE 1,Task2
val a=airportrdd.filter(x=>(x.split(",")(8)).toDouble%2==0).map(x=>x.split(",")(11))
a.countByValue

// COMMAND ----------

// DBTITLE 1,Task1
val filtered=airportrdd.filter(x=>(x.split(",")(7)).toDouble>40 || x.split(",")(3)=="\"Iceland\"")


// COMMAND ----------

filtered.saveAsTextFile("hfeey.csv")
