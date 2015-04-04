import com.databricks.spark.csv._
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf,SparkContext}

object nesting extends Serializable {
 def main(args: Array[String])  {
  val sc = new SparkContext(new SparkConf())

  val hc = new org.apache.spark.sql.hive.HiveContext(sc)

  val data = hc.csvFile("file:///data/data.csv")
  hc.registerRDDAsTable(data,"data")

  hc.sql("""CREATE TABLE IF NOT EXISTS nested (
   propertyId string,
   propertyName string,
   rooms array<struct<roomname:string,roomsize:int>>
) STORED AS PARQUET
""")
  
  val nestedRDD = data.map(buildRecord(_))
  
  // after the map, Spark does not know the schema
  // of the result RDD. We can just copy it from the
  // hive table using applySchema
  val nested = hc.sql("select * from nested limit 0")
  val schemaNestedRDD = hc.applySchema(nestedRDD, nested.schema)
  
  hc.registerRDDAsTable(schemaNestedRDD,"schemanestedrdd")

  hc.sql("insert overwrite table nested select * from schemanestedrdd")

 }

  def buildRecord(r:Row):Row = {
	println(r)
    var res  = Seq[Any]()
    res = res ++ r.slice(0,2) // takes the first two elements
                              // now res = [ 'some id','some name']
    var ary = Seq[Any]() // this will contain all the array elements
    for (i <- 0 to 1 ) {      // we assume there are 2 groups of columns
       // 0-based indexes, takes (2,3) (4,5) .. and converts to appropriate type
       ary = ary :+ Row( r.getString( 2 + 2 * i), r.getString(2 + 1 + 2*i).toInt )
    }
    res = res :+ ary // adds array as an element and returns it
    Row.fromSeq(res)
  }
}

