package rabbitmq;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class Solution1 {

    private final static String QUEUE_NAME = "q.transaction-authorizer";
    private final static String H_HASH_VALUE = "my-hash-value";

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
                HashMap<String, Object> map = new HashMap<>();
                map.put(H_HASH_VALUE, mHashValue);

                AMQP.BasicProperties props = new AMQP.BasicProperties
                        .Builder()
                        .headers(map)
                        .build();

                Map<String, Object> arguments = new HashMap<>();
                //arguments.put("x-single-active-consumer", true);

                channel.queueDeclare(QUEUE_NAME, /*durable*/false, /*exclusive*/false, /*autoDelete*/false, /*arguments*/arguments);

                for(int i=0;i<=5;i++) {
                    String message = "Hello World " + i;
                    channel.basicPublish(/*exchange*/"", /*routingKey*/QUEUE_NAME, props, message.getBytes(StandardCharsets.UTF_8));
                    System.out.println(" [x] Sent '" + message + "'");
                    sleep(2000);
                }
            } catch (TimeoutException | IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    static class SampleConsumer extends Thread {

        private final String mTag;
        private final String mHashValue;

        public SampleConsumer(String tag, String hashValue) {
            this.mTag = tag;
            this.mHashValue = hashValue;
        }

        public void run(){
            try {
                sleep(2000);
                System.out.println("--> Running consumer");

                ConnectionFactory factory = new ConnectionFactory();
                factory.setHost("localhost");
                Connection connection = factory.newConnection();
                Channel channel = connection.createChannel();

                Map<String, Object> arguments = new HashMap<>();
                //arguments.put("x-single-active-consumer", true);

                channel.queueDeclare(QUEUE_NAME, /*durable*/false, /*exclusive*/false, /*autoDelete*/false, /*arguments*/arguments);
                System.out.println(" [*] Waiting for messages....");

                DeliverCallback deliverCallback = (consumerTag, delivery) -> {

                    String message = new String(delivery.getBody(), "UTF-8");

                    if( delivery.getProperties().getHeaders().get(H_HASH_VALUE).toString().equals(this.mHashValue)) {
                        System.out.format("%s [x] Received '" + message + "'\n", mTag);
                        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                    }
                    else {
                        channel.basicReject(delivery.getEnvelope().getDeliveryTag(), true);
                    }

                };
                channel.basicConsume(QUEUE_NAME, /*autoAck*/false, deliverCallback, consumerTag -> { });

                sleep(Long.MAX_VALUE);

            } catch (InterruptedException | IOException | TimeoutException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] argv) throws Exception {

        SampleProducer producer = new SampleProducer("12");
        producer.start();

        SampleConsumer consumer1 = new SampleConsumer("First", "12");
        consumer1.start();

        SampleConsumer consumer2 = new SampleConsumer("Second", "13");
        consumer2.start();

        producer.join();
        consumer1.join();
        consumer2.join();

        System.out.println("Done");



    }

}
