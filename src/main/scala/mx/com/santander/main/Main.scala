package mx.com.santander.main

import mx.com.santander.etl.EtlIDLC
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {
    //TODO uso solo en local
     //val args = Array("C:\\Users\\z338195\\Documents\\Santander\\SmartNotifications\\Codigo\\rt-cloudera_core_batch\\src\\main\\resources\\datosConexion.properties")
    
    if (args.length < 1) {
      println("Put connection properties")
    } else {
      val spark = SparkSession.builder()
        //TODO solo usar en local
        //.master("local[*]")
        .getOrCreate()
      val etlIDLC = EtlIDLC(spark, args(0))
      etlIDLC.extraccion()
      etlIDLC.transformacion()
    }
  }

}