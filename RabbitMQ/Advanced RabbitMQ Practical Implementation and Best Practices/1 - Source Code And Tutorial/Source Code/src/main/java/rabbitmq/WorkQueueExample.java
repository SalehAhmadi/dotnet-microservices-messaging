package rabbitmq;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class WorkQueueExample {

    private static final String WORK_QUEUE_NAME = "work_queue";

    public static class Producer extends Thread {
        public void run() {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel())
            {
                channel.queueDeclare(WORK_QUEUE_NAME, true, false, false, null);

                for(int i=0;i<=5;i++) {
                    String message = "Hello World " + i;
                    channel.basicPublish("", WORK_QUEUE_NAME,
                            /*props*/ MessageProperties.PERSISTENT_TEXT_PLAIN,
                            message.getBytes("UTF-8"));
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
            try (
                    final Connection connection = factory.newConnection();
                    final Channel channel = connection.createChannel())
            {

                AMQP.Queue.DeclareOk rc = channel.queueDeclare(WORK_QUEUE_NAME, true, false, false, null);
                System.out.println(" [*] " + mWorkerName + " " + rc.getMessageCount() + " messages in the queue, waiting for messages....");

                channel.basicQos(/*prefetchCount*/1);

                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    String message = new String(delivery.getBody(), "UTF-8");

                    System.out.println(" [x] " + mWorkerName + " received '" + message + "'");
                    try {
                        //TODO do the work
                        try {
                            int randomInt = (int)(10.0 * Math.random());
                            Thread.sleep(1000 + randomInt * 10);
                        } catch (InterruptedException _ignored) {
                            Thread.currentThread().interrupt();
                        }
                    } finally {
                        System.out.println(" [x] " + mWorkerName + " processed, (ack)nowledging...");
                        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                        // channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, /*requeue*/true);
                        // channel.basicReject(delivery.getEnvelope().getDeliveryTag(), true);
                    }
                };
                channel.basicConsume(WORK_QUEUE_NAME, false, deliverCallback, consumerTag -> { });

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
