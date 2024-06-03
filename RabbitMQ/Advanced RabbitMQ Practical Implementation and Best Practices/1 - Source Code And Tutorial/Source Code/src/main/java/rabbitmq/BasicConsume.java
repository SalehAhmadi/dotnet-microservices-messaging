package rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class BasicConsume {

    static class SampleConsumer extends Thread {

        private final String mQueueName;

        public SampleConsumer(String queueName) {
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

                channel.queueDeclare(mQueueName, /*durable*/true, /*exclusive*/false, /*autoDelete*/false, /*arguments*/ /*args*/ null);
                System.out.println(" [*] Waiting for messages....");

                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    String message = new String(delivery.getBody(), "UTF-8");

                    System.out.println("%s [x] Received '" + message);
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

        SampleConsumer consumer = new SampleConsumer("q.test");
        consumer.start();

        consumer.join();

        System.out.println("Done");

    }

}
