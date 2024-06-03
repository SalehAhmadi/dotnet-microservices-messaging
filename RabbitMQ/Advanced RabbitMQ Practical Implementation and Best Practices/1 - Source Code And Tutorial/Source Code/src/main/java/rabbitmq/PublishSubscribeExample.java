package rabbitmq;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class PublishSubscribeExample {

    private static final String EXCHANGE_NAME = "logs";

    public static class Producer extends Thread {
        public void run() {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel())
            {
                channel.exchangeDeclare(EXCHANGE_NAME, "fanout");

                for(int i=0;i<=5;i++) {
                    String message = "Hello World " + i;
                    channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes("UTF-8"));
                    System.out.println(" [x] Sent '" + message + "'");
                    sleep(20);
                }

            } catch (TimeoutException | IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static class Worker extends Thread {

        private final String mWorkerName;

        public Worker(String workername) {
            this.mWorkerName = workername;
        }

        public void run() {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");

            try( Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel() )
            {

                channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
                String queueName = channel.queueDeclare().getQueue();
                AMQP.Queue.BindOk rc = channel.queueBind(queueName, EXCHANGE_NAME, /*routingKey*/"");

                System.out.println(" [*] " + mWorkerName + ", waiting for messages....");

                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    String message = new String(delivery.getBody(), "UTF-8");
                    System.out.println(" [x] " + mWorkerName + " received '" + message + "'");
                };
                channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });

                sleep(Long.MAX_VALUE);
            } catch (TimeoutException | IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] argv) throws Exception {

        Producer producer = new Producer();
        producer.start();

        Worker worker1 = new Worker("worker1");
        worker1.start();

        Worker worker2 = new Worker("worker2");
        worker2.start();

        producer.join();
        worker1.join();
        worker2.join();

        System.out.println("Done");
    }
}
