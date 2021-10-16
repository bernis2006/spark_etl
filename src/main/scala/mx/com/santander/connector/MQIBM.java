package mx.com.santander.connector;

import com.ibm.jms.JMSTextMessage;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import java.io.Serializable;
import java.util.Properties;

public class MQIBM extends Receiver<String> implements Serializable {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(MQIBM.class);
    
    private Properties properties;
    private int cont;
    private static int pivot = 0;
    
    public MQIBM(Properties properties) {
        super(StorageLevel.MEMORY_ONLY_2());
        properties.setProperty("pivot", "" + (++pivot));
        this.properties = properties;
    }

    @Override
    public void onStart() {
        new Thread() {
            @Override
            public void run() {
                receiveData(MQConnectionIBM.getConnection(properties));
            }
        }.start();
    }

    @Override
    public void onStop() {
    }

    //@Override
    public boolean sendMessage(String message) {
        boolean wasCorrect = true;
        try {
            LOGGER.info("Sending message [{}]" , message);
            MQClientIBMVO mqClientVO = MQConnectionIBM.getConnection(properties);
            JMSTextMessage messageToSend = (JMSTextMessage)mqClientVO.getSession().createTextMessage(message);
            mqClientVO.getProducer().send(messageToSend);
            cont = 0;
        } catch (JMSException ex) {
            LOGGER.error("Error to try to connect with IBMMQ, trying again. ERROR {}", ex);
            if (cont < 10) {
                cont++;
                sendMessage(message);
            } else {
                wasCorrect = false;
            }
        }
        return wasCorrect;
    }
    
    public MQClientIBMVO conecta() {
    	MQClientIBMVO mqClient = MQConnectionIBM.getConnection(properties);
    	return mqClient;
    }
    
    public void enviaMensaje(MQClientIBMVO mqClientVO, String message) {
    	try {
    		LOGGER.info("Envio de mensaje: [{}]" , message);
    		JMSTextMessage messageToSend = (JMSTextMessage)mqClientVO.getSession().createTextMessage(message);
    		mqClientVO.getProducer().send(messageToSend);
    	} catch (JMSException ex) {
            LOGGER.error("Error to try to connect with IBMMQ, trying again. ERROR {}", ex);
    	}
    }

    public void receiveData(MQClientIBMVO mqClientVO) {
        LOGGER.info("Started receiving messages from IBMMQ inicio: {}", mqClientVO);
        try {
            Message receivedMessage = null;
            
            while (!isStopped() && (receivedMessage = mqClientVO.getConsumer().receiveNoWait()) != null) {
                String userInput = ((TextMessage) receivedMessage).getText();
                store(userInput);
            }
            stop("No More Messages To read !");
            LOGGER.info("Queue Connection is Closed");

        } catch (JMSException e) {
            LOGGER.error("Error to try to connect with IBMMQ, trying again. ERROR {} ", e);
            restart("Trying to connect again");
        }
    }


}
