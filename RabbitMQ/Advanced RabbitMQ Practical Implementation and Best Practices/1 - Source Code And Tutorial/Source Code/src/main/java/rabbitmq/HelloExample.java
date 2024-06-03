package rabbitmq;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class HelloExample {
    private final static String QUEUE_NAME = "helloqueue";

    static class SampleProducer extends Thread {
        private final String mQueueName;

        public SampleProducer(String queueName){
            this.mQueueName = queueName;
        }

        public void run() {
            System.out.println("--> Running producer");
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel())
            {
                //alternative with TTL
                //Map<String, Object> args = new HashMap<String, Object>();
                //args.put("x-message-ttl", 60000);
                //channel.queueDeclare(QUEUE_NAME, false, false, false, args);


                channel.queueDeclare(QUEUE_NAME, /*durable*/false, /*exclusive*/false, /*autoDelete*/false, /*arguments*/null);
                for(int i=0;i<=5;i++) {
                    String message = "Hello World " + i;
                    channel.basicPublish(/*exchange*/"", /*routingKey*/QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
                    System.out.println(" [x] Sent '" + message + "'");
                    sleep(2000);

                    //alternative with TTL
                    //AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                    //        .expiration("60000")
                    //        .build();
                    //channel.basicPublish(/*exchange*/"", /*routingKey*/QUEUE_NAME, properties, message.getBytes(StandardCharsets.UTF_8));


                }
            } catch (TimeoutException | IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    static class SampleConsumer extends Thread {
        private final String mQueueName;

        public SampleConsumer(String queueName) {
            this.mQueueName = queueName;
        }

        public void run(){
            try {
                sleep(5000);
                System.out.println("--> Running consumer");

                ConnectionFactory factory = new ConnectionFactory();
                factory.setHost("localhost");
                Connection connection = factory.newConnection();
                Channel channel = connection.createChannel();

                channel.queueDeclare(QUEUE_NAME, /*durable*/false, /*exclusive*/false, /*autoDelete*/false, /*arguments*/null);
                System.out.println(" [*] Waiting for messages....");

                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    String message = new String(delivery.getBody(), "UTF-8");
                    System.out.println(" [x] Received '" + message + "'");
                };
                channel.basicConsume(QUEUE_NAME, /*autoAck*/true, deliverCallback, consumerTag -> { });

            } catch (InterruptedException | IOException | TimeoutException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] argv) throws Exception {

        SampleProducer producer = new SampleProducer(QUEUE_NAME);
        producer.start();

        SampleConsumer consumer = new SampleConsumer(QUEUE_NAME);
        consumer.start();

        producer.join();
        consumer.join();

        System.out.println("Done");



    }
}
