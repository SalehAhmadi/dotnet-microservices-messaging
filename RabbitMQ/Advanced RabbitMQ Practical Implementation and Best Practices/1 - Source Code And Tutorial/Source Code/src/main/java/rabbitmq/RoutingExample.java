package rabbitmq;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class RoutingExample {
    private static final String EXCHANGE_NAME = "direct_logs";

    public static class Producer extends Thread {
        public void run() {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel())
            {
                channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

                channel.basicPublish(EXCHANGE_NAME, "info", null, "Information message".getBytes("UTF-8"));
                channel.basicPublish(EXCHANGE_NAME, "warning", null, "Warning message".getBytes("UTF-8"));
                channel.basicPublish(EXCHANGE_NAME, "error", null, "Error message".getBytes("UTF-8"));
                channel.basicPublish(EXCHANGE_NAME, "critical", null, "Critical message".getBytes("UTF-8"));

                System.out.println(" [x] Sent 4 events");
            } catch (TimeoutException | IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class Consumer extends Thread {

        private final String mWorkerName;
        private final List<String> mLevels;

        public Consumer(String workername, List<String> levels) {
            this.mWorkerName = workername;
            this.mLevels = levels;
        }

        public void run() {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");

            try( Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel() )
            {
                channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
                String queueName = channel.queueDeclare().getQueue();



                for (String level : mLevels) {
                    channel.queueBind(queueName, EXCHANGE_NAME, level);
                }
                System.out.println(" [*] " + mWorkerName + " waiting for messages....");

                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    String message = new String(delivery.getBody(), "UTF-8");
                    System.out.println(" [x] " + mWorkerName + " received '" + delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
                };
                channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });

                sleep(Long.MAX_VALUE);

            } catch (TimeoutException | IOException| InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] argv) throws Exception {

        Consumer worker1 = new Consumer("worker_all", Arrays.asList("info","warning","error"));
        worker1.start();

        Consumer worker2 = new Consumer("worker_err", Arrays.asList("error"));
        worker2.start();

        Thread.sleep(2000);
        Producer producer = new Producer();
        producer.start();


        producer.join();
        worker1.join();
        worker2.join();

        System.out.println("Done");
    }
}
