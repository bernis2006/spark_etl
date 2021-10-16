package mx.com.santander.etl

import org.apache.spark.sql.{DataFrame, SparkSession}
import mx.com.santander.utilerias.{Utilerias}
import mx.com.santander.connector.MQIBM
import java.sql.{Connection, DriverManager, Statement, SQLException}

case class ETL_Blindaje (spark:SparkSession, urlConexion:String) {
  
   def extraccion():Unit={
   
    val datosConexion = new Utilerias().obtenFicheroDePropiedades(urlConexion)
    
    val credentialProviderPath = datosConexion.getProperty("credential.provider.path")
    val password = new Utilerias().getPasswordJceks(credentialProviderPath, "oracle.password")
    println("PASSWORD:" + password)
    //val password = "Dvp#e248320"
    datosConexion.setProperty("password", password)
    val url = "jdbc:oracle:thin:" + datosConexion.getProperty("usuario") + "/" + password + "@//" + datosConexion.getProperty("host") + ":" + datosConexion.getProperty("port") + "/" + datosConexion.getProperty("base") 
    
    val df_Blindaje = spark.read.format("jdbc")
      .option("url", url)
      .option("dbtable", datosConexion.getProperty("query"))
      .option("user", datosConexion.getProperty("usuario"))
      .option("password", datosConexion.getProperty("password"))
      .option("driver", datosConexion.getProperty("driver"))
      .load()
    
    df_Blindaje.createOrReplaceTempView("df_Blindaje")
    spark.sql("cache table df_Blindaje")
     
   }
   
   def transformacion():Unit ={

    val txtConexion = Utilerias().obtenFicheroDePropiedades(urlConexion)
    val mqMotor = new MQIBM(txtConexion)
    carga(spark.sql("SELECT JSON FROM df_Blindaje"), mqMotor)
    actualiza_base()
  }

  def carga(dataFrame: DataFrame, mqMotor:MQIBM):Unit={
    dataFrame.foreach(reg => mqMotor.sendMessage(reg.toString()))
    //dataFrame.foreach(reg => println(reg))
  }
  
  def actualiza_base() : Unit = {
    
    val datosConexion = new Utilerias().obtenFicheroDePropiedades(urlConexion)
    val credentialProviderPath = datosConexion.getProperty("credential.provider.path")
    val password = new Utilerias().getPasswordJceks(credentialProviderPath, "oracle.password")
    //val password = "Dvp#e248320"
      
        val cadena_conexion = "jdbc:oracle:thin:@" + datosConexion.getProperty("host") + ":" + datosConexion.getProperty("port") + ":" + datosConexion.getProperty("base")
        
        try {
            val conn = DriverManager.getConnection(cadena_conexion, datosConexion.getProperty("usuario"), password)
            val sentencia = conn.createStatement()
            val query = datosConexion.getProperty("query_Update")    
            val count = sentencia.executeUpdate(query)
        
            println("Número de registros actualizados:" + count)
            
            sentencia.close()
            conn.close()
        } catch  {
          case ex: SQLException => {
            println("SQLException: " + ex)
          }
        }
    
  }
   
  
}