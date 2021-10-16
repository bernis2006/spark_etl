package mx.com.santander.main

import org.apache.spark.sql.SparkSession
import mx.com.santander.etl.ETL_Blindaje

object Blindaje_Total {
  //System.setProperty("hadoop.home.dir", "C:\\Users\\Z258636\\RESPALDO\\Documentacion_Proyecto\\Winutils")
  
  def main (args: Array[String]): Unit = {
    
    //val args = Array("C:\\Users\\Z258636\\WfBauIDLC\\lib\\BlindajeTotal_desa.properties")
    
    if(args.length < 1) {
      println("Put connection properties")
    } else {
      //val spark = SparkSession.builder().master("local[*]").getOrCreate()
      val spark = SparkSession.builder().getOrCreate()
      val etl_blindaje = ETL_Blindaje(spark, args(0))
      etl_blindaje.extraccion()
      etl_blindaje.transformacion()
      
    }
  }
  
}