package mx.com.santander.utilerias

import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}


case class Utilerias(){
  def obtenFicheroDePropiedades(directorio:String):Properties={
    val properties = new Properties()
    try{
      val dirProperites = new Path(directorio)
      val fs = FileSystem.get(new Configuration())
      properties.load(fs.open(dirProperites))
    } catch {
      case excepcion: Exception => println("No se pudo realizar la lectura del fichero:" + excepcion)
    }
    properties
  }
  
   def getPasswordJceks(credentialProviderPath: String, keyCredential: String): String = {
     val spark = SparkContextSingleton.getSparkSession
     import spark.implicits._
     
     val configuration: Configuration = spark.sparkContext.hadoopConfiguration
     var password: String = ""
     
     try {
         configuration.set("hadoop.security.credential.provider.path", credentialProviderPath)
         val passwordChr = configuration.getPassword(keyCredential)
         password = new String(passwordChr);
     } catch {
       case e: Exception => println(e.getMessage)
     }
     
     password
 }
}

