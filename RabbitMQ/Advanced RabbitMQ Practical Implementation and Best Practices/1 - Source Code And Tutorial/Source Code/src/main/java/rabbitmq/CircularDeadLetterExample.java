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

public class CircularDeadLetterExample {

    private final static String DLX_EXCHANGE_NAME = "ex.dlx.messages";
    private final static String DLX_QUEUE_NAME = "q.dlx.messages";

    private final static String REGULAR_EXCHANGE = "ex.messages";
    private final static String REGULAR_QUEUE = "q.messages";

    private final static int NUMBER_OF_MESSGES = 1;
    private final static int DELAY_BETWEEN_MESSAGES = 2000; // in ms

    private final static int CONSUMER_INITAL_DEALY = 5000; // in ms
    private final static int CONSUMER_DELAY = 10000; // in ms

    static class SampleProducer extends Thread {

        public void run() {
            System.out.println("--> Running producer");
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel())
            {
                Map<String, Object> args1 = new HashMap<String, Object>();
                args1.put("x-dead-letter-exchange", REGULAR_EXCHANGE);
                args1.put("x-dead-letter-routing-key", "some-routing-key");

                channel.exchangeDeclare(DLX_EXCHANGE_NAME, "direct");   // dead letter exchange
                channel.queueDeclare(DLX_QUEUE_NAME, /*durable*/true, /*exclusive*/false, /*autoDelete*/false, /*arguments*/args1);
                channel.queueBind(DLX_QUEUE_NAME, DLX_EXCHANGE_NAME, "some-routing-key", null);


                Map<String, Object> args2 = new HashMap<String, Object>();
                args2.put("x-dead-letter-exchange", DLX_EXCHANGE_NAME);
                args2.put("x-dead-letter-routing-key", "some-routing-key");

                channel.exchangeDeclare(REGULAR_EXCHANGE, "direct");
                channel.queueDeclare(REGULAR_QUEUE, true, false, false, args2);
                channel.queueBind(REGULAR_QUEUE, REGULAR_EXCHANGE, "some-routing-key", null);

                for(int i=0;i<NUMBER_OF_MESSGES;i++) {
                    String message = "Hello World " + i;//System.currentTimeMillis();
                    channel.basicPublish(/*exchange*/REGULAR_EXCHANGE, /*routingKey*/"some-routing-key", null, message.getBytes(StandardCharsets.UTF_8));
                    System.out.println(" [x] Sent '" + message + "'");
                    sleep(DELAY_BETWEEN_MESSAGES);
                }
            } catch (TimeoutException | IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    static class SampleMessagesConsumer extends Thread {

        public void run(){
            try {
                sleep(CONSUMER_INITAL_DEALY);
                System.out.println("--> Running consumer");

                ConnectionFactory factory = new ConnectionFactory();
                factory.setHost("localhost");
                Connection connection = factory.newConnection();
                Channel channel = connection.createChannel();

                System.out.println(" [*] Waiting for messages....");

                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    String message = new String(delivery.getBody(), "UTF-8");
                    System.out.println(" [A] Received message '" + message + "' with routingKey=" + delivery.getEnvelope().getRoutingKey() + " - rejecting in 5s ....");
                    try {
                        sleep(CONSUMER_DELAY);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, /*requeue*/false);
                };
                channel.basicConsume(REGULAR_QUEUE, /*autoAck*/false, deliverCallback, consumerTag -> { });

                sleep(Long.MAX_VALUE);


            } catch (InterruptedException | IOException | TimeoutException e) {
                e.printStackTrace();
            }
        }
    }

    static class SampleDlxMessagesConsumer extends Thread {

        public void run(){
            try {
                sleep(CONSUMER_INITAL_DEALY);
                System.out.println("--> Running consumer for DLX queue");

                ConnectionFactory factory = new ConnectionFactory();
                factory.setHost("localhost");
                Connection connection = factory.newConnection();
                Channel channel = connection.createChannel();

                System.out.println(" [*] Waiting for rejected messages....");

                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    String message = new String(delivery.getBody(), "UTF-8");
                    String key = delivery.getEnvelope().getRoutingKey();
                    System.out.println(" [B] Received rejected message: '" + message + "' with routingKey=" + key + " -> " + delivery.getProperties().toString() + " - rejecting again in 5s ....");
                    try {
                        sleep(CONSUMER_DELAY);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, /*requeue*/false);
                };
                channel.basicConsume(DLX_QUEUE_NAME, /*autoAck*/false, deliverCallback, consumerTag -> { });

                sleep(Long.MAX_VALUE);

            } catch (InterruptedException | IOException | TimeoutException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] argv) throws Exception {

        SampleProducer producer = new SampleProducer();
        producer.start();

        Thread.sleep(2000);

        SampleMessagesConsumer consumer1 = new SampleMessagesConsumer();
        consumer1.start();

        SampleDlxMessagesConsumer consumer2 = new SampleDlxMessagesConsumer();
        consumer2.start();

        producer.join();
        consumer1.join();
        consumer2.join();

        System.out.println("Done");

    }
}
