package mx.com.santander.connector;

import com.ibm.mq.jms.MQMessageConsumer;
import com.ibm.mq.jms.MQMessageProducer;
import com.ibm.mq.jms.MQQueueConnection;
import com.ibm.mq.jms.MQQueueSession;

import java.io.Serializable;

public class MQClientIBMVO implements Serializable{
    private MQMessageConsumer consumer;
    private MQQueueConnection connection;
    private MQQueueSession session;
    private MQMessageProducer producer;

    public MQMessageConsumer getConsumer() {
        return consumer;
    }

    public void setConsumer(MQMessageConsumer consumer) {
        this.consumer = consumer;
    }

    public MQQueueConnection getConnection() {
        return connection;
    }

    public void setConnection(MQQueueConnection connection) {
        this.connection = connection;
    }

    public MQQueueSession getSession() {
        return session;
    }

    public void setSession(MQQueueSession session) {
        this.session = session;
    }

    public MQMessageProducer getProducer() {
        return producer;
    }

    public void setProducer(MQMessageProducer producer) {
        this.producer = producer;
    }
    
    @Override
    public String toString() {
        return "MQClientVO{" + "consumer=" + consumer + ", connection=" + connection + ", session=" + session + ", producer=" + producer + '}';
    }

        
}

