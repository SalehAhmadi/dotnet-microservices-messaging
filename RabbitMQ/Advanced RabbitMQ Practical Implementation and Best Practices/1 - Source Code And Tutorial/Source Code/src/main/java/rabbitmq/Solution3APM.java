package rabbitmq;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.concurrent.TimeoutException;

public class Solution3APM {

    private final static String EXCHANGE_NAME1 = "ex.global";
    private final static String QUEUE_NAME = "q.transaction-authorizer";
    private final static String EXCHANGE_NAME2 = "ex.transaction";

    static class SampleProducer extends Thread {

        public void run() {
            System.out.println("--> Running producer");
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel())
            {
                channel.queueDeclare(QUEUE_NAME, /*durable*/true, /*exclusive*/false, /*autoDelete*/false, /*arguments*/null);
                channel.exchangeDeclare(EXCHANGE_NAME1, "direct", true, false, null);
                channel.exchangeDeclare(EXCHANGE_NAME2, "direct", true, false, null);

                channel.exchangeBind(EXCHANGE_NAME2, EXCHANGE_NAME1, "");
                channel.queueBind(QUEUE_NAME, EXCHANGE_NAME2, "");

                for(int i=0;i<=5;i++) {

                    HashMap<String, Object> map = new HashMap<>();
                    map.put("ts1", System.currentTimeMillis());
                    AMQP.BasicProperties props = new AMQP.BasicProperties
                            .Builder()
                            .headers(map)
                            .build();

                    String message = "Hello World " + i;
                    channel.basicPublish(/*exchange*/EXCHANGE_NAME1, /*routingKey*/"", props, message.getBytes(StandardCharsets.UTF_8));
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
        private final String mQueueName;

        public SampleConsumer(String tag, String queueName, String hashValue) {
            this.mTag = tag;
            this.mHashValue = hashValue;
            this.mQueueName = queueName;
        }

        public void run(){
            try {
                sleep(2000);
                System.out.println("--> Running consumer");

                ConnectionFactory factory = new ConnectionFactory();
                factory.setHost("localhost");
                Connection connection = factory.newConnection();
                Channel channel = connection.createChannel();

                channel.queueDeclare(mQueueName, /*durable*/true, /*exclusive*/false, /*autoDelete*/false, /*arguments*/null);

                DeliverCallback deliverCallback = (consumerTag, delivery) -> {

                    String message = new String(delivery.getBody(), "UTF-8");
                    String fromExchange = delivery.getEnvelope().getExchange();
                    long ts1 = Long.parseLong(delivery.getProperties().getHeaders().get("ts1").toString());
                    long diff = System.currentTimeMillis() - ts1;
                    System.out.format("%s [x] Received '" + message + "' from exchange '%s', diff=%d ms \n", mTag, fromExchange, diff);
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);

                };
                channel.basicConsume(mQueueName, /*autoAck*/false, deliverCallback, consumerTag -> { });

                sleep(Long.MAX_VALUE);

            } catch (InterruptedException | IOException | TimeoutException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] argv) throws Exception {

        SampleProducer producer = new SampleProducer();
        producer.start();

        SampleConsumer consumer1 = new SampleConsumer("First", QUEUE_NAME, "12");
        consumer1.start();

        //SampleConsumer consumer2 = new SampleConsumer("Second", "q.test-queue13", "13");
        //consumer2.start();

        producer.join();
        consumer1.join();
        //consumer2.join();

        System.out.println("Done");



    }

}
