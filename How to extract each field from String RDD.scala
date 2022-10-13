//		How to extract each field from String RDD

case class DepartureDelay(date:Int, delay: Int, distance :Int, origin :String, dest :String)
//function to get seperate fields
def getFields(s:String):DepartureDelay={
  val arr = s.split(",")
  return DepartureDelay(arr(0).toInt,
                        arr(1).toInt,
                        arr(2).toInt,
                        arr(3).toString,
                        arr(4).toString)
}
val filePath:String = "dbfs:/databricks-datasets/flights/departuredelays.csv"

val rdd = spark.sparkContext.textFile(filePath)
val header = rdd.first()
//skip header from data
val data = rdd.filter(row => row != header)
val rdd1:RDD[DepartureDelay]= data.map(r => getFields(r.toString))
rdd1.take(2).foreach(
  row => println("Date=%d, Delay=%d, Distance=%d, Origin=%s, Dest=%s"
                 .format(row.date,row.delay,row.distance,row.origin,row.dest))
)
