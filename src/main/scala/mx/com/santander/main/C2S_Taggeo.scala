package mx.com.santander.main

import mx.com.santander.etl.ETL_C2ST
import org.apache.spark.sql.SparkSession

object C2S_Taggeo {
    def main(args: Array[String]): Unit = {

     //val args = Array("C:\\Users\\Z258636\\WfBauIDLC\\lib\\C2S_TAGGEO_desa.properties")
    
    if (args.length < 1) {
      println("Put connection properties")
      
    } else {
      //val spark = SparkSession.builder().master("local[*]").getOrCreate()
      val spark = SparkSession.builder().getOrCreate()
      val etl_C2ST = ETL_C2ST(spark, args(0))
      etl_C2ST.extraccion()
      etl_C2ST.transformacion()
    }
  }
}