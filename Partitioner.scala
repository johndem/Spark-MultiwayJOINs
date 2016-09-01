
import org.apache.spark.SparkConf
import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.RDD

class Partitioner extends Serializable {
  
  var red_id = "000"  //  initial reducer ID
  
  //  arrays of Strings to hold each relation respectively(A, B, C, R)
  var linesA = ArrayBuffer[String]()
  var linesB = ArrayBuffer[String]()
  var linesC = ArrayBuffer[String]()
  var linesR = ArrayBuffer[String]()
  
  
  //  receive arrays of Strings containing relations R, A, B, C and assign them to the respective local arrays to initialize
  def Partitioner(rddR: ArrayBuffer[String], rddA: ArrayBuffer[String], rddB: ArrayBuffer[String], rddC: ArrayBuffer[String]) {
    
    linesR = rddR
    linesA = rddA
    linesB = rddB
    linesC = rddC
    
  }
  
  
  //  perform join operation on the relations R, A, B, C in this reducer by first checking which records belong here, filtering out those that doesn't
  //  and finally performing join on the filtered relations R, A, B, C like in Part 1
  def join(sc: SparkContext, mA: Int, mB: Int, mC: Int): RDD[String] = {
    
    // create rdds for relations A, B, C, R by parallelizing their respective arrays of Strings into RDDs of Strings
    val Ardd = sc.parallelize(linesA)
    val Brdd = sc.parallelize(linesB)
    val Crdd = sc.parallelize(linesC)
    val Rrdd = sc.parallelize(linesR)
    
    
    //  transform A's RDD of Strings into (key,value) pairs where key is each record's bucket position e.g. (0,1,0) as was calculated in the main function
    //  after using a hashfunction on A's initial key and determining a combination of its other two coordinates based on B's and C's shares
    //  and value is the record's two initial fields
    val a = Ardd.map(x => (x.split(",")(0) + "," + x.split(",")(1) + "," + x.split(",")(2), x.split(",")(3) + "," + x.split(",")(4)))
    //  if each record from A has a key matching the reducer's ID, keep it, otherwise remove it
    val aa = a.filter(x => {
      if (red_id == x._1) {
        true
      }
      else {
        false
      }
      
    })
    
    //  transform B's RDD of Strings into (key,value) pairs where key is each record's bucket position e.g. (1,1,0) as was calculated in the main function
    //  after using a hashfunction on B's initial key and determining a combination of its other two coordinates based on A's and C's shares
    //  and value is the record's two initial fields
    val b = Brdd.map(x => (x.split(",")(0) + "," + x.split(",")(1) + "," + x.split(",")(2), x.split(",")(3) + "," + x.split(",")(4)))
    //  if each record from B has a key matching the reducer's ID, keep it, otherwise remove it
    val bb = b.filter(x => {
      if (red_id == x._1) {
        true
      }
      else {
        false
      }
      
    })
    
    //  transform C's RDD of Strings into (key,value) pairs where key is each record's bucket position e.g. (0,0,1) as was calculated in the main function
    //  after using a hashfunction on C's initial key and determining a combination of its other two coordinates based on A's and B's shares
    //  and value is the record's two initial fields
    val c = Crdd.map(x => (x.split(",")(0) + "," + x.split(",")(1) + "," + x.split(",")(2), x.split(",")(3) + "," + x.split(",")(4)))
    //  if each record from A has a key matching the reducer's ID, keep it, otherwise remove it
    val cc = c.filter(x => {
      if (red_id == x._1) {
        true
      }
      else {
        false
      }
      
    })
    
    //  transform R's RDD of Strings into (key,value) pairs where key will be the record's bucket position e.g. (1,0,1) by using a hashfunction
    //  on R's first three fields(exluding the value field), first field modulo A's share, second field modulo B's share, third field modulo C's share
    //  and value will be the whole record(all four initial fields a,b,c,value)
    val r = Rrdd.map(x => (computeKey(x.split(",")(0).toInt, mA) + "," + computeKey(x.split(",")(1).toInt, mB) + "," + computeKey(x.split(",")(2).toInt, mC), x))
    //  if each record from R has a key matching the reducer's ID, keep it, otherwise remove it
    val rr = r.filter(x => {
      if (red_id == x._1) {
        true
      }
      else {
        false
      }
      
    })
    

    //  after filtering the RDDs, transform their records into appropriate (key,value) pairs like in Part 1
    // A's key will be field a and value will be field x - A(a,x)
    val finalArdd = aa.map(x => (x._2.split(",")(0), x._2.split(",")(1)))
    // B's key will be field b and value will be field y - B(b,y)
    val finalBrdd = bb.map(x => (x._2.split(",")(0), x._2.split(",")(1)))
    // C's key will be field c and value will be field z - C(c,z)
    val finalCrdd = cc.map(x => (x._2.split(",")(0), x._2.split(",")(1)))
    // R's key will be field a to ensure join with relation A and value will be fields b,c,value - R(a,b,c,value)
    val finalRrdd = rr.map(x => (x._2.split(",")(0), x._2.split(",")(0) + "," + x._2.split(",")(1) + "," + x._2.split(",")(2) + "," + x._2.split(",")(3)))
    
    
    // join R to A to produce RA and transform each tuple result into string
    val RjoinAstrings = finalRrdd.join(finalArdd).values.map { x => x.toString().substring(1, x.toString().length()-1) }
    // transform above RDD to appropriate (key, value) pairs
    val RjoinA = RjoinAstrings.map(x => (x.split(",")(1), x))
    
    // join RA to B to produce RAB and transform each tuple result into string
    val RAjoinBstrings = RjoinA.join(finalBrdd).values.map { x => x.toString().substring(1, x.toString().length()-1) }
    // transform above RDD to appropriate (key, value) pairs
    val RAjoinB = RAjoinBstrings.map(x => (x.split(",")(2), x))
    
    // join RAB to C to produce RABC which is the final JOIN result
    val RABjoinCstrings = RAjoinB.join(finalCrdd).values.map { x => x.toString().substring(1, x.toString().length()-1) }
		
    
    //  return to main a single RDD for this reducer containing the join result it produced
    val final_result = RABjoinCstrings.coalesce(1)
    return final_result
    
  }
  
  
  //  function to calculate the result of a hashfunction in the form of key modulo number m
  def computeKey(key: Int, m: Int): Int = {
    return key % m
  }
  
}