package mx.com.santander.connector;

import com.ibm.mq.jms.*;
import com.ibm.msg.client.wmq.WMQConstants;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import java.util.HashMap;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MQConnectionIBM {
	private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(MQConnectionIBM.class);
    private static HashMap<Integer, MQClientIBMVO> connections = new HashMap<>();

    private MQConnectionIBM() {
        throw new IllegalStateException("MQ Connection class");
    }
    
    public static synchronized MQClientIBMVO getConnection(Properties properties){
        int pivot = Integer.parseInt(properties.getProperty("pivot"));
        MQClientIBMVO mqClientVO = connections.get(pivot);

        if (mqClientVO == null || mqClientVO.getConnection() == null || (mqClientVO.getConsumer() == null && properties.getProperty("ibmmq.receiver.name") != null) || (mqClientVO.getProducer() == null && properties.getProperty("ibmmq.producer.name") != null) ) {
            try {
                LOGGER.info("************ Getting connection to IBMMQ with access: User: [{}], Host: [{}], Port: [{}], Channel: [{}], " + (properties.getProperty("ibmmq.producer.name") == null ? "" : "Producer queue: [{}]") + (properties.getProperty("ibmmq.receiver.name") == null ? "" : ", Receiver queue: [{}],")
                		,properties.getProperty("ibmmq.access.user")
                		,properties.getProperty("ibmmq.access.host")
                		,properties.getProperty("ibmmq.access.port")
                		,properties.getProperty("ibmmq.access.channel")
                		,properties.getProperty("ibmmq.producer.name")
                		,properties.getProperty("ibmmq.receiver.name"));
                mqClientVO = initConnection(properties);
                connections.put(pivot, mqClientVO);
            } catch (JMSException ex) {
                Logger.getLogger(MQConnectionIBM.class.getName()).log(Level.SEVERE, null, ex);
                
            }

        }

        return connections.get(pivot);
    }

    private static MQClientIBMVO initConnection(Properties properties) throws JMSException {
        MQQueueConnection connection;
        MQQueue queueProducer;
        MQMessageProducer producer;
        MQMessageConsumer consumer;
        MQQueue queueConsumer;
        MQQueueSession session;
        MQClientIBMVO mqClientVO = new MQClientIBMVO();
        MQQueueConnectionFactory conFactory = new MQQueueConnectionFactory();
        conFactory.setHostName(properties.getProperty("ibmmq.access.host"));
        conFactory.setPort(Integer.parseInt(properties.getProperty("ibmmq.access.port")));
        conFactory.setIntProperty(WMQConstants.WMQ_CONNECTION_MODE, WMQConstants.WMQ_CM_CLIENT);
        conFactory.setChannel(properties.getProperty("ibmmq.access.channel"));
        //conFactory.setBooleanProperty(WMQConstants.USER_AUTHENTICATION_MQCSP, true);
        conFactory.setStringProperty(WMQConstants.USERID, properties.getProperty("ibmmq.access.user"));
        //conFactory.setStringProperty(WMQConstants.PASSWORD, properties.getProperty(PropertyConstants.IBMMQ_ACS_PASS));
        
        connection = (MQQueueConnection) conFactory.createConnection(); 
        session = (MQQueueSession) connection.createQueueSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE); 
            
        if(properties.getProperty("ibmmq.receiver.name") != null){
            queueConsumer = (MQQueue) session.createQueue(properties.getProperty("ibmmq.receiver.name"));
            consumer = (MQMessageConsumer) session.createConsumer(queueConsumer); 
            mqClientVO.setConsumer(consumer);
        }
        if(properties.getProperty("ibmmq.producer.name") != null){
            queueProducer = (MQQueue) session.createQueue(properties.getProperty("ibmmq.producer.name")); 
            producer = (MQMessageProducer) session.createProducer(queueProducer); 
            mqClientVO.setProducer(producer);
        }
        
        mqClientVO.setConnection(connection);
        mqClientVO.setSession(session);

        connection.start();
        return mqClientVO;
    }

}
