package mx.com.santander.main

import mx.com.santander.etl.ETL_BAU
import org.apache.spark.sql.SparkSession


object LoadLeoBuc {

  def main(args: Array[String]): Unit = {
    //TODO uso solo en local
   // val args = Array("C:\\Users\\Z424413\\Downloads\\Transformation_Files\\local\\oracle-dev.properties")

    if (args.length < 1) {
      println("Put connection properties")
    } else {
      val spark = SparkSession.builder()
        //TODO solo usar en local
       // .master("local[*]")
        .getOrCreate()
      val etlClientes = ETL_BAU(spark, args(0))
      etlClientes.loadBau()
    }
  }

}