package mx.com.santander.etl

import java.text.SimpleDateFormat

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.time.LocalDateTime
import java.util.{Date, Properties, UUID}
import org.apache.spark.sql.functions.{expr, col, collect_list}
import mx.com.santander.connector.MQIBM
import mx.com.santander.utilerias.{Utilerias}

case class ETL_SM(spark:SparkSession, urlConexion:String) {
  
   def extraccion():Unit={
    val datosConexion = new Utilerias().obtenFicheroDePropiedades(urlConexion)
    
    val credentialProviderPath = datosConexion.getProperty("credential.provider.path")
    val password = new Utilerias().getPasswordJceks(credentialProviderPath, "oracle.password")
    //val password = "Dvp#151049"
    datosConexion.setProperty("password", password)
    val url = "jdbc:oracle:thin:" + datosConexion.getProperty("usuario") + "/" + password + "@//" + datosConexion.getProperty("host") + ":" + datosConexion.getProperty("port") + "/" + datosConexion.getProperty("base") 
    
    val datos_SM = spark.read.format("jdbc")
      .option("url", url)
      .option("dbtable", datosConexion.getProperty("query"))
      .option("user", datosConexion.getProperty("usuario"))
      .option("password", datosConexion.getProperty("password"))
      .option("driver", datosConexion.getProperty("driver"))
      .load()
    datos_SM.createOrReplaceTempView("datos_SM")
    spark.sql("cache table datos_SM")
  }

  def transformacion():Unit ={
    spark.udf.register("uuid",() => UUID.randomUUID.toString)
    spark.udf.register("obtenFecha",() => LocalDateTime.now().toString)

   val datos_SM = spark.sql("select  struct(cast(NUM_CLIENTE AS BigInt) AS NUM_CLIENTE, UNIVERSO, NOMBRE, AP_PATERNO, SEGMENTO, cast(BIENVENIDA AS BigInt) AS BIENVENIDA, cast(TOQUE1 AS BigInt) AS TOQUE1, cast(TOQUE2 AS BigInt) AS TOQUE2) as eventData, " ++
                       "        struct(uuid() as eventId, cast(cast(NUM_CLIENTE as BigInt) as String) as keyValue, 'BUC' as keyName, 'LSM' as refTrans, obtenFecha() as timestamp) as eventHeader," ++
                       "        struct('default' as IDLC, struct() as MCI) as campaigns, struct('TI' as `MST_TARJETAS_RT_CORE$TIPO_CTE`) as data_integration" ++
                       "        from datos_SM")
                       
    val df_SM  = datos_SM
    .groupBy(col("eventData"), col("eventHeader"),col("campaigns"))
    .agg(collect_list("data_integration").as("data_integration"))
    .select(expr("""struct(campaigns, data_integration) AS contextData"""), col("eventData"),col("eventHeader"))
    
    val txtConexion = Utilerias().obtenFicheroDePropiedades(urlConexion)
    val mqMotor = new MQIBM(txtConexion)
    carga(df_SM, mqMotor)
  }

  def carga(dataFrame: DataFrame, mqMotor:MQIBM):Unit={
    dataFrame.toJSON.foreach(reg => mqMotor.sendMessage(reg))
    dataFrame.toJSON.foreach(rdd => println(rdd))
  }

}