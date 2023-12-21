package mx.com.santander.etl

import mx.com.santander.utilerias.Utilerias
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes._


case class ETL_Clients(spark:SparkSession, urlConexion:String) {
  
  def extraccion():Unit={
    val datosConexion = Utilerias().obtenFicheroDePropiedades(urlConexion)
    val tabla_Final = datosConexion.getProperty("tabla_Final")
    val credentialProviderPath = datosConexion.getProperty("credential.provider.path")
    val password = Utilerias().getPasswordJceks(credentialProviderPath, "oracle.password")
    datosConexion.setProperty("password", password)
    val url = "jdbc:oracle:thin:" + datosConexion.getProperty("usuario") + "/" + password + "@//" + datosConexion.getProperty("host") + ":" + datosConexion.getProperty("port") + "/" + datosConexion.getProperty("base") 
    println("ALF:" + url)
    val df_Clientes = spark.read.format("jdbc")
      .option("url", url)
      .option("dbtable", datosConexion.getProperty("query"))
      .option("user", datosConexion.getProperty("usuario"))
      .option("password", datosConexion.getProperty("password"))
      .option("driver", datosConexion.getProperty("driver"))
      .load()
    val new_clientes = df_Clientes.
      withColumn("COD_PLASTICO", regexp_replace( df_Clientes("COD_PLASTICO").cast(StringType), "\\..*",""))
      .withColumn("SMS", df_Clientes("SMS").cast(IntegerType))
      .withColumn("EMAIL", df_Clientes("EMAIL").cast(IntegerType))
    new_clientes.show();
    new_clientes.write.mode("overwrite").saveAsTable(tabla_Final);
    
  }
}

