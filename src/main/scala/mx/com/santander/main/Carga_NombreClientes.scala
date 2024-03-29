package mx.com.santander.main

import mx.com.santander.etl.ETL_Clients
import org.apache.spark.sql.SparkSession


object Carga_NombreClientes {

  def main(args: Array[String]): Unit = {
    //TODO uso solo en local
    val args = Array("C:\\Users\\Z424413\\Downloads\\Transformation_Files\\local\\oracle-dev.properties")
    
    if (args.length < 1) {
      println("Put connection properties")
    } else {
      val spark = SparkSession.builder()
        //TODO solo usar en local
        .master("local[*]")
        .getOrCreate()
      val etlClientes = ETL_Clients(spark, args(0))
      etlClientes.extraccion()
    }
  }

}