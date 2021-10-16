package mx.com.santander.utilerias;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkContextSingleton {

    private static final Logger LOGGER = LoggerFactory.getLogger(SparkContextSingleton.class);
    private static SparkSession sparkSession;
    private static JavaStreamingContext strmCnt;
    
    private SparkContextSingleton() {
        throw new IllegalStateException("Singleton class");
    }

    public static final synchronized  SparkSession getSparkSession() {

        if (sparkSession != null) {
            LOGGER.info("sparkSession.sparkContext().isStopped()? : {}", sparkSession.sparkContext().isStopped());
        }

        if (sparkSession == null || sparkSession.sparkContext().isStopped()) {
            initializeSparkContext();
        }

        return sparkSession;
    }
    
    public static final JavaStreamingContext getStreamingContext(int duration) {
        if (strmCnt == null) {
        	getSparkSession();
        	strmCnt = new JavaStreamingContext(new JavaSparkContext(sparkSession.sparkContext()), new Duration(duration));
        }
        return strmCnt;
    }
    
    public static void closeSession() {
    	sparkSession.close();
    }

    private static void initializeSparkContext() {
        LOGGER.info("***** initializing spark session");
        sparkSession = SparkSession.builder()
        		//.master("local[*]")
        		.getOrCreate();
    }
    

}
