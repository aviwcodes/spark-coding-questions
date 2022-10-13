val filePath:String = "dbfs:/databricks-datasets/flights/departuredelays.csv"
val df =spark.read
.option("header","true")
.option("inferSchema","true")
.csv(filePath)

df.limit(2).show()
println(df.schema.toDDL)
