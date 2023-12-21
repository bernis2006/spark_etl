package mx.com.santander.etl

import mx.com.santander.utilerias.Utilerias
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}


case class ETL_BAU(spark:SparkSession, urlConexion:String) {


  def loadBau(): Unit = {
    val pathHDFS = Utilerias().obtenFicheroDePropiedades( urlConexion )
    val tabla_Buc = pathHDFS.getProperty("tabla_buc" )
    val tabla_Lego = pathHDFS.getProperty("tabla_lego" )

    val path_bu = pathHDFS.getProperty("provider.path_buc" )
    val path_lego = pathHDFS.getProperty("provider.path_lego" )

    // Configuración de Hadoop FileSystem
    val hadoopConf = new Configuration()
    val hdfs = FileSystem.get( hadoopConf )

    // Ruta de entrada y salida en HDFS
    val inputPath = new Path(path_bu)

    val inputPathLego = new Path(path_lego)

    // Leer datos de la tabla desde HDFS
    val inputData: DataFrame = spark.read.format( "parquet" ).load( inputPath.toString )

    val inputDataLego: DataFrame = spark.read.format( "parquet" ).load( inputPathLego.toString )

   // val inputDataTarjetas: DataFrame = spark.read.format( "text" ).load( inputPathTarjetas.toString )

    // Imprimir el esquema y los primeros registros de la tabla de entrada

    inputData.printSchema()
    inputData.show()

    inputDataLego.printSchema()
    inputDataLego.show()

    // Guardar datos en otra tabla en HDFS
    inputData.write.format("parquet").mode("overwrite").saveAsTable(tabla_Buc)
    inputDataLego.write.format("parquet").mode("overwrite").saveAsTable(tabla_Lego)
    // inputDataTarjetas.write.format("parquet").saveAsTable("sb_rt.rtcore_n915024_tarjeta_digital")

    // Cerrar sesión de Spark
    spark.stop()

  }


}
