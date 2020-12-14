// Databricks notebook source
lazy val a = 10
a

// COMMAND ----------

lazy val variable_lazy = {println("hello world");5}
variable_lazy
variable_lazy

// COMMAND ----------

lazy val sum=10+b
val b=9
println(sum)

// COMMAND ----------

val `my name is tushar` = "goyal"

// COMMAND ----------

def square(a:Int): Int ={
  return a*a
}
square(2)

// COMMAND ----------

val `void`=100
val `false`=true
val `return` =90
if(`false`) `void` else `return`

// COMMAND ----------

def square(a:Int):Int = {
  a*a
}
def sq2(y:Int,functions: Int => Int) : Int={
  functions(y) 
}

sq2(2,square)

// COMMAND ----------


