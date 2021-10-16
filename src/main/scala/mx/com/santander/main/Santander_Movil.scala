package mx.com.santander.main

import mx.com.santander.etl.ETL_SM
import org.apache.spark.sql.SparkSession

object Santander_Movil {
  def main(args: Array[String]): Unit = {

     //val args = Array("C:\\Users\\Z258636\\WfBauIDLC\\lib\\SantanderMovil_desa.properties")
    
    if (args.length < 1) {
      println("Put connection properties")
      
    } else {
      //val spark = SparkSession.builder().master("local[*]").getOrCreate()
      val spark = SparkSession.builder().getOrCreate()
      val etl_SM = ETL_SM(spark, args(0))
      etl_SM.extraccion()
      etl_SM.transformacion()
    }
  }
  
}
