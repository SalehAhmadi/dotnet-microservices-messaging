package rabbitmq;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class TopicsExample {

    private static final String EXCHANGE_NAME = "topic_logs";

    public static class Producer extends Thread {
        public void run() {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel())
            {
                channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

                channel.basicPublish(EXCHANGE_NAME, "aplication1.logs.error", null, "Sample Error Message from App1".getBytes("UTF-8"));
                channel.basicPublish(EXCHANGE_NAME, "aplication1.logs.info", null, "Sample Info Message from App1".getBytes("UTF-8"));

                channel.basicPublish(EXCHANGE_NAME, "aplication2.logs.error", null, "Sample Error Message from App2".getBytes("UTF-8"));
                channel.basicPublish(EXCHANGE_NAME, "aplication2.logs.debug", null, "Sample Debug Message from App2".getBytes("UTF-8"));

                System.out.println(" [x] Sent 4 example messages");

            } catch (TimeoutException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class Consumer extends Thread{
        private final List<String> mTopics;
        private final String mWorkerName;

        public Consumer(String name, List<String> topics) {
            this.mTopics = topics;
            this.mWorkerName = name;
        }

        public void run() {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            try(
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel())
            {
                channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
                String queueName = channel.queueDeclare().getQueue();

                for (String bindingKey : mTopics) {
                    channel.queueBind(queueName, EXCHANGE_NAME, bindingKey);
                }

                System.out.println(" [*] " + mWorkerName + " waiting for messages...");

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

        Consumer worker1 = new Consumer("worker_all_errors", Arrays.asList("*.logs.error"));
        worker1.start();

        Consumer worker2 = new Consumer("worker_app1", Arrays.asList("aplication1.#"));
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
