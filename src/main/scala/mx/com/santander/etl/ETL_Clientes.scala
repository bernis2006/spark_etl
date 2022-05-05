package mx.com.santander.etl

import mx.com.santander.connector.MQIBM
import mx.com.santander.utilerias.Utilerias
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime
import java.util.UUID


case class ETL_Clientes(spark:SparkSession, urlConexion:String) {
  
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
    df_Clientes.show();
    df_Clientes.write.mode("overwrite").saveAsTable(tabla_Final);
    
  }
}

