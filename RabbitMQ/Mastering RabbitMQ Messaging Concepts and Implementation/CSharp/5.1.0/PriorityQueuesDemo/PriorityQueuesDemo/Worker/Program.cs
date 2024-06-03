using System;
using System.Threading;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Worker
{
    class Program
    {
        static void Main(string[] args)
        {
            ConnectionFactory factory = new ConnectionFactory();
            // "guest"/"guest" by default, limited to localhost connections
            factory.HostName = "localhost";
            factory.VirtualHost = "/";
            factory.Port = 5672;
            factory.UserName = "guest";
            factory.Password = "guest";

            IConnection conn = factory.CreateConnection();
            IModel channel = conn.CreateModel();

            //Don't send a new message to this worker 
            //until it has processed and acknowledged the last one
            channel.BasicQos(0, 1, false);

            //Create consumers to receive messages
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (sender, e) =>
            { 
                string message = System.Text.Encoding.UTF8.GetString(e.Body);                

                Console.Write($"Processing message -> '{ message}' ...");
                Thread.Sleep(1000);
                Console.WriteLine("FINISHED");                

                channel.BasicAck(e.DeliveryTag, false);
            };

            //Subscribe to channel
            String consumerTag = channel.BasicConsume(
                                        "my.queue", //Queue name
                                        false, //Auto Ack
                                        consumer);

            Console.WriteLine("Subscribed to the queue. Waiting for messages");
            Console.ReadKey();

            //Unsubscribe
            channel.BasicCancel(consumerTag);

            channel.Close();
            conn.Close();
        }
    }
}
