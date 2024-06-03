package rabbitmq;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class HeadersExample {

    private static final String EXCHANGE_NAME = "header_logs_exchange";
    //private static final String QUEUE_NAME = "header_logs_queue";

    public static class Producer extends Thread {
        public void run() {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel())
            {
                channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.HEADERS);
                /*String queueName = channel.queueDeclare().getQueue();

                HashMap<String,Object> map = new HashMap<String,Object>();
                map.put("x-match","any");
                channel.queueBind(queueName, EXCHANGE_NAME, "" ,map);*/

                publishSampleMessage(channel, "error", "Sample Error Message from App1");
                publishSampleMessage(channel, "info", "Sample Info Message from App1");
                publishSampleMessage(channel, "error", "Sample Error Message from App2");
                publishSampleMessage(channel, "debug", "Sample Debug Message from App2");

                System.out.println(" [x] Sent 4 example messages");

            } catch (TimeoutException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private void publishSampleMessage(Channel channel, String header, String message) throws IOException {
            HashMap<String, Object> map = new HashMap<>();
            map.put("x-match","any");   //all or any
            map.put("my-header-severity", header);
            map.put("my-custom-header", "hello");
            AMQP.BasicProperties props = new AMQP.BasicProperties
                    .Builder()
                    .headers(map)
                    .build();

            channel.basicPublish(EXCHANGE_NAME, /*routingKey*/"", props, message.getBytes("UTF-8"));
        }
    }

    public static class Consumer extends Thread{
        private final List<String> mHeaders;
        private final String mWorkerName;

        public Consumer(String name, List<String> headers) {
            this.mHeaders = headers;
            this.mWorkerName = name;
        }

        public void run() {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            try(
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel())
            {
                channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.HEADERS);
                String queueName = channel.queueDeclare().getQueue();
                //channel.queueDeclare(QUEUE_NAME, true, false, false, null);

                for (String header : mHeaders) {
                    HashMap<String,Object> map = new HashMap<String,Object>();
                    map.put("x-match","any");   //all or any
                    map.put("my-header-severity",header);
                    map.put("my-custom2","bambo");
                    channel.queueBind(queueName, EXCHANGE_NAME, "", map);   //Builders are also available Binding binding = BindingBuilder.bind(queueName).to(EXCHANGE_NAME).where("my-header-severity").matches(header);
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

        Consumer worker1 = new Consumer("worker_errors", Arrays.asList("error"));
        worker1.start();

        Consumer worker2 = new Consumer("worker_info_warn", Arrays.asList("info", "warn"));
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
