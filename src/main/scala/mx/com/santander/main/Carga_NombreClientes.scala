package mx.com.santander.main

import mx.com.santander.etl.{ETL_Clientes}
import org.apache.spark.sql.SparkSession

object Carga_NombreClientes {

  def main(args: Array[String]): Unit = {
    //TODO uso solo en local
    //val args = Array("C:\\Users\\z338195\\Documents\\Santander\\SmartNotifications\\Documentacion\\Respaldo Original Smart Notifications\\Transformation_Files\\PRE_local_Carga_clientes\\Carga_Clientes-pre.properties")
    
    if (args.length < 1) {
      println("Put connection properties")
    } else {
      val spark = SparkSession.builder()
        //TODO solo usar en local
        //.master("local[*]")
        .getOrCreate()
      val etlClientes = ETL_Clientes(spark, args(0))
      etlClientes.extraccion()
    }
  }

}