package rabbitmq;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class DeadLetterExample {
    /*private final static String DLX_NAME = "dlxexchange";
    private final static String DLX_KEY = "some-routing-key";
    private final static String DLX_QUEUE = "dlxqueue";

    private final static String EXCHANGE = "messages_exchange";
    private final static String CONSUMER_QUEUE_1 = "consumer_queue";
    private final static String CONSUMER_EXC_KEY = "key1";*/

    //private final static String EXCHANGE_NAME = "some.exchange.name";
    //private final static String QUEUE_NAME = "dlxqueue";

    private final static String DLX_EXCHANGE_NAME = "some.exchange.name";
    private final static String DLX_QUEUE_NAME = "dlxqueue2";
    //private final static String QUEUE_NAME = "dlxqueue";

    private final static String RETRY_EXCHANGE = "RetryExchange";
    private final static String RETRY_QUEUE = "producerqueue";
    private final static int RETRY_DELAY = 10000; // in ms


    private final static int NUMBER_OF_MESSGES = 6;
    private final static int DELAY_BETWEEN_MESSAGES = 2000; // in ms

    private final static int CONSUMER_DEALY = 5000; // in ms

    static class SampleProducer extends Thread {

        public void run() {
            System.out.println("--> Running producer");
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel())
            {
                channel.exchangeDeclare(DLX_EXCHANGE_NAME, "direct");   // dead letter exchange
                channel.queueDeclare(DLX_QUEUE_NAME, /*durable*/true, /*exclusive*/false, /*autoDelete*/false, /*arguments*/null);
                channel.queueBind(DLX_QUEUE_NAME, DLX_EXCHANGE_NAME, "some-routing-key", null);


                Map<String, Object> args = new HashMap<String, Object>();
                args.put("x-dead-letter-exchange", DLX_EXCHANGE_NAME);
                args.put("x-message-ttl", RETRY_DELAY );
                args.put("x-dead-letter-routing-key", "some-routing-key");

                channel.exchangeDeclare(RETRY_EXCHANGE, "direct");
                channel.queueDeclare(RETRY_QUEUE, true, false, false, args);
                channel.queueBind(RETRY_QUEUE, RETRY_EXCHANGE, "", null);

                for(int i=0;i<=NUMBER_OF_MESSGES;i++) {
                    String message = "Hello World " + i;//System.currentTimeMillis();
                    channel.basicPublish(/*exchange*/RETRY_EXCHANGE, /*routingKey*/"", null, message.getBytes(StandardCharsets.UTF_8));
                    System.out.println(" [x] Sent '" + message + "'");
                    sleep(DELAY_BETWEEN_MESSAGES);
                }
            } catch (TimeoutException | IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    static class SampleConsumer extends Thread {

        public void run(){
            try {
                sleep(CONSUMER_DEALY);
                System.out.println("--> Running consumer");

                ConnectionFactory factory = new ConnectionFactory();
                factory.setHost("localhost");
                Connection connection = factory.newConnection();
                Channel channel = connection.createChannel();

                /*Map<String, Object> args = new HashMap<String, Object>();
                args.put("x-dead-letter-exchange", EXCHANGE_NAME);
                args.put("x-dead-letter-routing-key", "some-routing-key");*/
                //channel.queueDeclare(QUEUE_NAME, /*durable*/false, /*exclusive*/false, /*autoDelete*/false, /*arguments*/args);
                System.out.println(" [*] Waiting for messages....");

                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    String message = new String(delivery.getBody(), "UTF-8");
                    System.out.println(" [x] Rejecting '" + message + "' with routingKey=" + delivery.getEnvelope().getRoutingKey());
                    channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, /*requeue*/false);
                };
                channel.basicConsume(RETRY_QUEUE, /*autoAck*/false, deliverCallback, consumerTag -> { });

                sleep((NUMBER_OF_MESSGES-2)*DELAY_BETWEEN_MESSAGES - CONSUMER_DEALY);    //kill before last 2 messages to simulate TTL

                System.out.println("--> Closing consumer to simulate TTL");
                channel.close();
                System.out.println("--> Consumer closed");


            } catch (InterruptedException | IOException | TimeoutException e) {
                e.printStackTrace();
            }
        }
    }

    static class RejectorHandlerConsumer extends Thread {

        public void run(){
            try {
                System.out.println("--> Running consumer for rejected messages");

                ConnectionFactory factory = new ConnectionFactory();
                factory.setHost("localhost");
                Connection connection = factory.newConnection();
                Channel channel = connection.createChannel();

                /*channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
                String queueName = channel.queueDeclare().getQueue();*/
                System.out.println(" [*] Waiting for rejected messages....");

                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    String message = new String(delivery.getBody(), "UTF-8");
                    String key = delivery.getEnvelope().getRoutingKey();
                    System.out.println(" [x] Received rejected message: '" + message + "' with routingKey=" + key + " -> " + delivery.getProperties().toString());
                    //System.out.println(" [x] Received rejected message: '" + message + "' -> " + delivery.getEnvelope().isRedeliver()
                };
                channel.basicConsume(DLX_QUEUE_NAME, /*autoAck*/true, deliverCallback, consumerTag -> { });

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

        SampleConsumer consumer = new SampleConsumer();
        consumer.start();

        RejectorHandlerConsumer rejectorHandler = new RejectorHandlerConsumer();
        rejectorHandler.start();

        producer.join();
        consumer.join();
        rejectorHandler.join();

        System.out.println("Done");

    }
}
