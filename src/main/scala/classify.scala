import com.sun.glass.ui.Window.Level
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.rdd.RDD

import scala.io.Source






object classify {





  def main(args: Array[String]): Unit = {
    if(args.length == 0) {
      println("You forgot input arguments")
    } else if(args.length != 3) {
      println("Three arguments are needed")
    }

    // Read in arguments
    val argsList = args.toList
    val tweet = Source.fromFile(args(1)).mkString

    val trainingfile = argsList.head
    val outputName = argsList(2)
    val conf = new SparkConf().setAppName("Geographical estimation of tweet").setMaster("local[*]")
    // conf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    // conf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
    val sc = new SparkContext(conf)
    val data: RDD[String] = sc.textFile(trainingfile)
    val initialization = new calculate_probabilities(data,tweet,outputName)
    initialization.mainTransformFunction()

    // Spark setup and read

  }

}
