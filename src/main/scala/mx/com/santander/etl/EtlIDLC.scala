package mx.com.santander.etl

import java.text.SimpleDateFormat

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.time.LocalDateTime
import java.util.{Date, Properties, UUID}

import mx.com.santander.connector.MQIBM
import mx.com.santander.utilerias.{Utilerias}


case class EtlIDLC(spark:SparkSession, urlConexion:String) {
  
  def extraccion():Unit={
    val datosConexion = new Utilerias().obtenFicheroDePropiedades(urlConexion)
    //TODO usar en prod
    val credentialProviderPath = datosConexion.getProperty("credential.provider.path")
    val password = new Utilerias().getPasswordJceks(credentialProviderPath, "oracle.password")
    //val password = "Pre15Feb#19_sqm"
    datosConexion.setProperty("password", password)
    val url = "jdbc:oracle:thin:" + datosConexion.getProperty("usuario") + "/" + password + "@//" + datosConexion.getProperty("host") + ":" + datosConexion.getProperty("port") + "/" + datosConexion.getProperty("base") 
    
    val datosIDLC = spark.read.format("jdbc")
      .option("url", url)
      .option("dbtable", datosConexion.getProperty("tabla"))
      .option("user", datosConexion.getProperty("usuario"))
      .option("password", datosConexion.getProperty("password"))
      .option("driver", datosConexion.getProperty("driver"))
      .load()
    datosIDLC.createOrReplaceTempView("datosIDLC")
    spark.sql("cache table datosIDLC")
    
  }

  def transformacion():Unit ={
    spark.udf.register("uuid",() => UUID.randomUUID.toString)
    spark.udf.register("obtenFecha",() => LocalDateTime.now().toString)

    val datosBauIDLC = spark.sql("select  struct(cast(cast(COD_PLASTICO as BigInt) as String) as TJT_PAN_NU_01) as eventData, " ++
                       "        struct(uuid() as eventId, cast(cast(COD_PLASTICO as BigInt) as String) as keyValue, 'PAN' as keyName, 'IDLC_BAU' as refTrans, obtenFecha() as timestamp) as eventHeader," ++
                       "        struct(struct(struct(date_format(to_timestamp(FEC_FIN_CAMP, 'yyyy-MM-dd hh:mm:ss'), 'dd/MM/yyyy') as fechaVigencia, cast(cast(INCREMENTO as BigInt) as String) as mtoIncremento, TERM_PAN as panmask, NOM_PRODUCTO as nomProducto, cast(cast(NUM_CONTRATO as BigInt) as String) as numContrato, cast(cast(COD_CENTRO_ALTA as Int) as String) as codSucursal, cast(cast(NUM_CLIENTE as bigint) as String) as codCliente, cast(cast(LIM_CRED_ACTUAL AS BigInt) as String) AS limCredito, cast(cast(COD_SUBPROD as BigInt) as String) as idSubProducto, cast(cast(COD_PRODUCTO as Int) as String) as idProducto, COD_PLASTICO_TOKEN as pan, cast(cast(LIM_CRED_FINAL as BigInt) as String) as limFinal) as IDLC, struct() as MCI) as campaigns, array(named_struct('MST_CONTRATOS_RT_CORE$NUM_CONTRATO',cast(cast(NUM_CONTRATO as BigInt) as String),'MST_TARJETAS_RT_CORE$ULT_DIG_TARJ',TERM_PAN, 'MST_TARJETAS_RT_CORE$COD_SUBPROD', cast(cast(COD_SUBPROD as BigInt) as String), 'MST_TARJETAS_RT_CORE$COD_PRODUCTO', cast(cast(COD_PRODUCTO as Int) as String), 'MST_CLIENTES_RT_CORE$BUC', cast(cast(NUM_CLIENTE as BigInt) as String), 'MST_TARJETAS_RT_CORE$TIPO_CTE','TI', 'MST_TARJETAS_RT_CORE$COD_PLASTICO', cast(cast(COD_PLASTICO as BigInt) as String))) as data_integration) as contextData" ++
                       "                           from datosIDLC")
    val txtConexion = Utilerias().obtenFicheroDePropiedades(urlConexion)
    val mqMotor = new MQIBM(txtConexion)
    carga(datosBauIDLC, mqMotor)
  }

  def carga(dataFrame: DataFrame, mqMotor:MQIBM):Unit={
    dataFrame.toJSON.foreach(reg => mqMotor.sendMessage(reg))
  }

}

