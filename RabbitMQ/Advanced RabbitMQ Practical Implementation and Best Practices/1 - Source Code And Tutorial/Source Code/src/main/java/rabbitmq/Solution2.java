package rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class Solution2 {

    private static final String CONSISTENT_HASH_EXCHANGE = "ex.consistent-hash";
    //private final static String QUEUE_NAME = "q.transaction-authorizer";
    private final static String H_HASH_VALUE = "my-hash-value";
    private final static String DLX_EXCHANGE_NAME = "ex.dlx";

    static class SampleProducer extends Thread {


        private final String mHashValue;

        public SampleProducer(String hashValue) {
            this.mHashValue = hashValue;
        }

        public void run() {
            System.out.println("--> Running producer");
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel())
            {

                channel.exchangeDeclare(CONSISTENT_HASH_EXCHANGE, "x-consistent-hash", true, false, null);
                //channel.queueBind(QUEUE_NAME, CONSISTENT_HASH_EXCHANGE, "1");

                sleep(2000);
                //channel.queueDeclare(QUEUE_NAME, /*durable*/false, /*exclusive*/false, /*autoDelete*/false, /*arguments*/null);
                //channel.txSelect();
                for(int i=0;i<=5;i++) {
                    String message = "Hello World " + i;
                    channel.basicPublish(/*exchange*/CONSISTENT_HASH_EXCHANGE, /*routingKey*/mHashValue, null, message.getBytes(StandardCharsets.UTF_8));
                    System.out.println(" [x] Sent '" + message + "'");
                    sleep(2000);
                }
                //channel.txCommit();
            } catch (TimeoutException | IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    static class SampleConsumer extends Thread {

        private final String mTag;
        private final String mHashValue;
        private final String mQueueName;

        public SampleConsumer(String tag, String queueName, String hashValue) {
            this.mTag = tag;
            this.mHashValue = hashValue;
            this.mQueueName = queueName;
        }

        public void run(){
            try {

                System.out.println("--> Running consumer");

                ConnectionFactory factory = new ConnectionFactory();
                factory.setHost("localhost");
                factory.setPort(8672);
                factory.setUsername("test");
                factory.setPassword("test");
                Connection connection = factory.newConnection();
                Channel channel = connection.createChannel();

                //Map<String, Object> args = new HashMap<String, Object>();
                //args.put("x-dead-letter-exchange", DLX_EXCHANGE_NAME);

                channel.queueDeclare(mQueueName, /*durable*/true, /*exclusive*/false, /*autoDelete*/false, /*arguments*/ /*args*/ null);
                System.out.println(" [*] Waiting for messages....");
                //channel.queueBind(mQueueName, CONSISTENT_HASH_EXCHANGE, mHashValue);

                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    //channel.txSelect();
                    String message = new String(delivery.getBody(), "UTF-8");

                    System.out.format("%s [x] Received '" + message + "'\n", mTag);
                    //channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                    channel.basicReject(delivery.getEnvelope().getDeliveryTag(), true);

                };
                channel.basicConsume(mQueueName, /*autoAck*/false, deliverCallback, consumerTag -> { });

                sleep(Long.MAX_VALUE);

            } catch (InterruptedException | IOException | TimeoutException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] argv) throws Exception {

        //SampleProducer producer = new SampleProducer("12");
        //producer.start();

        //SampleConsumer consumer1 = new SampleConsumer("First", "q.test-queue12", "12");
        //consumer1.start();

        SampleConsumer consumer2 = new SampleConsumer("Second", "q.test1", "13");
        consumer2.start();

        //producer.join();
        //consumer1.join();
        consumer2.join();

        System.out.println("Done");



    }

}
