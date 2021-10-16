package mx.com.santander.etl

import java.text.SimpleDateFormat

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.time.LocalDateTime
import java.util.{Date, Properties, UUID}
import org.apache.spark.sql.functions.{expr, col, collect_list}
import mx.com.santander.connector.MQIBM
import mx.com.santander.utilerias.{Utilerias}

case class ETL_C2ST(spark:SparkSession, urlConexion:String) {
     def extraccion():Unit={
    val datosConexion = new Utilerias().obtenFicheroDePropiedades(urlConexion)
    
    val credentialProviderPath = datosConexion.getProperty("credential.provider.path")
    val password = new Utilerias().getPasswordJceks(credentialProviderPath, "oracle.password")
    //val password = "Dvp#151049"
    datosConexion.setProperty("password", password)
    val url = "jdbc:oracle:thin:" + datosConexion.getProperty("usuario") + "/" + password + "@//" + datosConexion.getProperty("host") + ":" + datosConexion.getProperty("port") + "/" + datosConexion.getProperty("base") 
    
    val datos_C2S = spark.read.format("jdbc")
      .option("url", url)
      .option("dbtable", datosConexion.getProperty("query"))
      .option("user", datosConexion.getProperty("usuario"))
      .option("password", datosConexion.getProperty("password"))
      .option("driver", datosConexion.getProperty("driver"))
      .load()
    datos_C2S.createOrReplaceTempView("datos_C2ST")
    spark.sql("cache table datos_C2ST")
    
  }

  def transformacion():Unit ={
    spark.udf.register("uuid",() => UUID.randomUUID.toString)
    spark.udf.register("obtenFecha",() => LocalDateTime.now().toString)

    
    val datos_C2ST = spark.sql("select  struct(cast(NUM_CLIENTE AS BigInt) AS NUM_CLIENTE, PRODUCTO, TIPO, CAST(NUM_TOQUE AS BigInt) AS NUM_TOQUE, date_format(to_timestamp(FIN_VIGENCIA, 'yyyy-MM-dd hh:mm:ss'), 'dd/MM/yyyy') as FIN_VIGENCIA, NOMBRE, APELLIDO) as eventData, " ++
                       "        struct(uuid() as eventId, cast(cast(NUM_CLIENTE as BigInt) as String) as keyValue, 'BUC' as keyName, 'C2ST' as refTrans, obtenFecha() as timestamp) as eventHeader," ++
                       "        struct('default' as IDLC, struct() as MCI) as campaigns, struct('TI' as `MST_TARJETAS_RT_CORE$TIPO_CTE`) as data_integration" ++
                       "        from datos_C2ST")
    
    val df_C2ST  = datos_C2ST
    .groupBy(col("eventData"), col("eventHeader"),col("campaigns"))
    .agg(collect_list("data_integration").as("data_integration"))
    .select(expr("""struct(campaigns, data_integration) AS contextData"""), col("eventData"),col("eventHeader"))
    
    val txtConexion = Utilerias().obtenFicheroDePropiedades(urlConexion)
    val mqMotor = new MQIBM(txtConexion)
    carga(df_C2ST, mqMotor)
  }

  def carga(dataFrame: DataFrame, mqMotor:MQIBM):Unit={
    dataFrame.toJSON.foreach(reg => mqMotor.sendMessage(reg))
    dataFrame.toJSON.foreach(rdd => println(rdd))
  }
}