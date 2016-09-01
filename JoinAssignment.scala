
import org.apache.spark.SparkConf
import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import scala.math
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType}

object Join {
  
  def main(args: Array[String]) = {
    
    //  program arguments given by user
    val inputFile = args(0) //  input file
    val outputFile1 = args(1) //  output folder for Part 1
    val outputFile2 = args(2) //  output folder for Part 2
    val outputFile3 = args(3) //  output folder for Part 2
    val numOfReducers = args(4) //  number of reducers
    
    //System.setProperty("hadoop.home.dir", "D:\\winutil\\")
    
    //  initialization of the spark context
    val conf = new SparkConf().setAppName("JoinAssignment").setMaster("local")
    val sc = new SparkContext(conf)
    
    //  k is the number of reducers
    val k = numOfReducers.toInt
    
    //  create an array of strings for each relation A, B, C, R
    var linesA = ArrayBuffer[String]()
    var linesB = ArrayBuffer[String]()
    var linesC = ArrayBuffer[String]()
    var linesR = ArrayBuffer[String]()
    
    //  for each line of the input file, determine relation it originates from by checking the first character and then assign line to the respective
    //  array(A or B or C or R) while also removing the first two characters(relation name and comma)
    for(line <- Source.fromFile(inputFile).getLines()) {
      if (line.charAt(0) == 'A') {
        linesA += line.substring(2)  //  add line to A's array
      }
      else if (line.charAt(0) == 'B') {
        linesB += line.substring(2)  //  add line to B's array
      }
      else if (line.charAt(0) == 'C') {
        linesC += line.substring(2)  //  add line to C's array
      }
      else {
        linesR += line.substring(2)  //  add line to R's array
      }
    }
    
    
    //  create RDDs for relations A, B, C, R by parallelizing the above arrays, thus providing an RDD for each relation containing
    //  all its records
    val rddA = sc.parallelize(linesA)
    val rddB = sc.parallelize(linesB)
    val rddC = sc.parallelize(linesC)
    val rddR = sc.parallelize(linesR)
    
    
    /************************************************** Part 1 - Basic (key, value) pair JOIN *************************************************/
    
    //  begin timer
    val part1t0 = System.nanoTime()
    
    
    //  transform initial RDDs to (key, value) pair RDDs by setting the first number as key and the rest as value
    val finalArdd = rddA.map(x => (x.split(",")(0), x.split(",")(1)))
    val finalBrdd = rddB.map(x => (x.split(",")(0), x.split(",")(1)))
    val finalCrdd = rddC.map(x => (x.split(",")(0), x.split(",")(1)))
    //  let R's key be its first field to ensure join condition with relation A at first
    val finalRrdd = rddR.map(x => (x.split(",")(0), x))

    
    // join R to A to produce RA and transform each tuple result into string and remove first and last characters which are parentheses
    val RjoinAstrings = finalRrdd.join(finalArdd).values.map { x => x.toString().substring(1, x.toString().length()-1) }
    // transform above RDD to appropriate (key, value) pairs where key is R's second field, to ensure join with B and value is the whole record
    val RjoinA = RjoinAstrings.map(x => (x.split(",")(1), x))
    
    // join RA to B to produce RAB and transform each tuple result into string and remove first and last characters which are parentheses
    val RAjoinBstrings = RjoinA.join(finalBrdd).values.map { x => x.toString().substring(1, x.toString().length()-1) }
    // transform above RDD to appropriate (key, value) pairs where key is R's third field, to ensure join with C and value is the whole record
    val RAjoinB = RAjoinBstrings.map(x => (x.split(",")(2), x))
    
    // join RAB to C to produce RABC which is the final JOIN result by first removing first and last characters which are parentheses
    val RABjoinCstrings = RAjoinB.join(finalCrdd).values.map { x => x.toString().substring(1, x.toString().length()-1) }
		
    
    //  stop timer
    val part1t1 = System.nanoTime()
    println("Elapsed time for Part 1: " + (part1t1-part1t0)/1000000 + " ms")
    println("Number of results for Part 1: " + RABjoinCstrings.count())
    
    
    //  return a single RDD containing the result
    val part1result = RABjoinCstrings.coalesce(1)
    //  write result in output file - each result record is in the shape a,b,c,value,x,y for R(a,b,c,value), A(a,x), B(b,y), C(c,z)
    part1result.saveAsTextFile(outputFile1)
    
    
    /************************************************** Part 2 - SQL table JOIN *************************************************/
    
    //  begin timer
    val part2t0 = System.nanoTime()
    
    
    //  initialize Spark SQL context
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    
    
    //  define the tuple/record schema for each relation as a string
    val schemaStringA = "a x"
    val schemaStringB = "b y"
    val schemaStringC = "c z"
    val schemaStringR = "a b c value"
    
    //  generate A's schema based on the string of its schema
    val schemaA = StructType(schemaStringA.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    //  convert tuples of A's RDD to rows
    val rowARDD = rddA.map(_.split(",")).map(p => Row(p(0), p(1).trim))
    //  apply the above schema to A's RDD by creating a DataFrame
    val relationDataFrameA = sqlContext.createDataFrame(rowARDD, schemaA)
    //  register A's DataFrame as a table
    relationDataFrameA.registerTempTable("relationA")
    
    //  generate B's schema based on the string of its schema
    val schemaB = StructType(schemaStringB.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    //  convert tuples of B's RDD to rows
    val rowBRDD = rddB.map(_.split(",")).map(p => Row(p(0), p(1).trim))
    //  apply the above schema to B's RDD by creating a DataFrame
    val relationDataFrameB = sqlContext.createDataFrame(rowBRDD, schemaB)
    //  register B's DataFrame as a table
    relationDataFrameB.registerTempTable("relationB")
    
    //  generate C's schema based on the string of its schema
    val schemaC = StructType(schemaStringC.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    //  convert tuples of C's RDD to rows
    val rowCRDD = rddC.map(_.split(",")).map(p => Row(p(0), p(1).trim))
    //  apply the above schema to C's RDD by creating a DataFrame
    val relationDataFrameC = sqlContext.createDataFrame(rowCRDD, schemaC)
    //  register C's DataFrame as a table
    relationDataFrameC.registerTempTable("relationC")
    
    //  generate R's schema based on the string of its schema
    val schemaR = StructType(schemaStringR.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    //  convert tuples of R's RDD to rows
    val rowRRDD = rddR.map(_.split(",")).map(p => Row(p(0), p(1), p(2), p(3).trim))
    //  apply the above schema to R's RDD by creating a DataFrame
    val relationDataFrameR = sqlContext.createDataFrame(rowRRDD, schemaR)
    //  register R's DataFrame as a table
    relationDataFrameR.registerTempTable("relationR")
    
    
    //  execute join in the form of an SQL query
    val results = sqlContext.sql("SELECT relationR.a, relationR.b, relationR.c, relationR.value, relationA.x, relationB.y, relationC.z FROM relationR JOIN relationA ON relationR.a = relationA.a JOIN relationB ON relationR.b = relationB.b JOIN relationC ON relationR.c = relationC.c")  
    
    
    // stop timer
    val part2t1 = System.nanoTime()
    println("Elapsed time for Part 2: " + (part2t1-part2t0)/1000000 + " ms")
    println("Number of results for Part 2: " + results.count())
    
    
    //  return a single RDD containing the result, as opposed to the default of 200
    val part2result = results.map { t => t.toString() }.coalesce(1)
    //  write result in output file - each result record is in the shape [a,b,c,value,x,y for R(a,b,c,value), A(a,x), B(b,y), C(c,z)]
    part2result.saveAsTextFile(outputFile2)
    
    
    /************************************************** Part 3 - multiway STAR JOIN *************************************************/
    
    //  begin timer
    val part3t0 = System.nanoTime()
    
    
    //
    val shareA = math.ceil(linesA.length * math.cbrt(k*1.0 / (linesA.length * linesB.length * linesC.length)))
    val shareB = math.ceil(linesB.length * math.cbrt(k*1.0 / (linesA.length * linesB.length * linesC.length)))
    val shareC = math.ceil(linesC.length * math.cbrt(k*1.0 / (linesA.length * linesB.length * linesC.length)))
    
    //  ensure reducer shares are Integers
    val mA = shareA.toInt
    val mB = shareB.toInt
    val mC = shareC.toInt
    

    //  arrays for A, B, C to hold the transformed records
    var newAline = ArrayBuffer[String]()
    var newBline = ArrayBuffer[String]()
    var newCline = ArrayBuffer[String]()
    
    
    //  for each record in A, create a new key like (h(a),y,z), e.g. (0,1,0) where h is a hashfunction and y,z take values from 0 to B's and C's shares respectively
    //  assign A's two fields as value
    for (i <- 0 until linesA.length) {
      val key = linesA(i).split(",")(0).toInt
      val newKey = key % mA  //  hashfunction is A's key modulo A's share
      
      for (j <- 0 until mB) {
        for (z <- 0 until mC) {
          //  assign new key and value to record's string
          newAline += newKey.toString() + "," + j.toString() + "," + z.toString() + "," + linesA(i).split(",")(0) + "," + linesA(i).split(",")(1)
        }
      }
    }
    
    //  for each record in B, create a new key like (x,h(b),z), e.g. (1,1,0) where h is a hashfunction and x,z take values from 0 to A's and C's shares respectively
    //  assign B's two fields as value
    for (i <- 0 until linesB.length) {
      val key = linesB(i).split(",")(0).toInt
      val newKey = key % mB  //  hashfunction is B's key modulo B's share

      for (j <- 0 until mA) {
        for (z <- 0 until mC) {
          newBline += j.toString() + "," + newKey.toString() + "," +  z.toString() + "," + linesB(i).split(",")(0) + "," + linesB(i).split(",")(1)
        }
      }
    }
    
    //  for each record in C, create a new key like (x,y,h(c)), e.g. (0,0,1) where h is a hashfunction and x,y take values from 0 to A's and B's shares respectively
    //  assign C's two fields as value
    for (i <- 0 until linesC.length) {
      val key = linesC(i).split(",")(0).toInt
      val newKey = key % mC  //  hashfunction is C's key modulo C's share

      for (j <- 0 until mA) {
        for (z <- 0 until mB) {
          newCline += j.toString() + "," + z.toString() + "," + newKey.toString() + "," + linesC(i).split(",")(0) + "," + linesC(i).split(",")(1)
        }
      }
    }
    
    
    // create an array of objects representing Reducers(here named Partinioners) with a size consisting of the product of A's, B's and C's shares
    var reducers = new Array[Partitioner](mA*mB*mC)
    
    var reducer_id = ""
    var counter = 0
    
    for (i <- 0 until mA) {
      for (j <- 0 until mB) {
        for (z <- 0 until mC) {
          reducer_id = i.toString() + "," + j.toString() + "," + z.toString()  //  determine an ID for each reducer consisting of a unique combination based on A's, B's and C's shares
          reducers(counter) = new Partitioner  //  initialize new Reducer
          reducers(counter).red_id = reducer_id  //  assign ID to reducer
          reducers(counter).Partitioner(linesR, newAline, newBline, newCline)  //  call some constructor to send the whole relations R, A, B, C to the reducer to be filtered
          counter += 1
        }
      }
    }
    
    
    //  an array containing the RDDs created by the reducers(here named partitioners)
    var reducer_rdds = ArrayBuffer[RDD[String]]()
    
    //  for every reducer, execute a join operation on its data(RDDs for R, A, B, C) and return the end product of join
    reducers.foreach { x => reducer_rdds+=x.join(sc, mA, mB, mC) }
    
    //  perform a union operation to combine all join results from each reducer into a single RDD
    for (i <- 1 until reducer_rdds.length) {
      reducer_rdds(0) = reducer_rdds(0).union(reducer_rdds(i))
    }
    
    
    //  stop timer
    val part3t1 = System.nanoTime()
    println("Elapsed time for Part 3: " + (part3t1-part3t0)/1000000 + " ms")
    
    //  assign single RDD containing union of the reducers' join results into final result RDD
    val part3result = reducer_rdds(0)
    
    println("Number of results for Part 3: " + part3result.count())
    println(mA + " reducers for A, " + mB + " reducers for B, " + mC + " reducers for C.")
    
    
    //  write result in output file
    part3result.saveAsTextFile(outputFile3)
    
  }
  
  
  //  function to calculate the result of a hashfunction in the form of key modulo number m
  def computeKey(key: Int, m: Int): Int = {
    return key % m
  }
  
}


