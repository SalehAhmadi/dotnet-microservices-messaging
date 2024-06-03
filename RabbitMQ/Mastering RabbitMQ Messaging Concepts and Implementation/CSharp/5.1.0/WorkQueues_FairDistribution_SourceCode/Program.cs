using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace WorkQueuesDemo
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.Write("Enter the name for this worker:");
            string workerName = Console.ReadLine();

            ConnectionFactory factory = new ConnectionFactory();
            // "guest"/"guest" by default, limited to localhost connections
            factory.HostName = "localhost";
            factory.VirtualHost = "/";
            factory.Port = 5672;
            factory.UserName = "guest";
            factory.Password = "guest";

            IConnection conn = factory.CreateConnection();
            IModel channel = conn.CreateModel();

            channel.BasicQos(0, 1, false);

            //Create consumers to receive messages
            var consumer = new EventingBasicConsumer(channel);            
            consumer.Received += (sender, e) =>
            {
                string message = System.Text.Encoding.UTF8.GetString(e.Body);
                int durationInSeconds = Int32.Parse(message);

                Console.Write("["+workerName+"] Task Started. Duration: " + durationInSeconds);

                Thread.Sleep(durationInSeconds * 1000);

                Console.WriteLine("FINISHED");

                channel.BasicAck(e.DeliveryTag, false);
            };

            String consumerTag = channel.BasicConsume("my.queue1", false, consumer);

            Console.WriteLine("Subscribed to the queue. Press a key to exit");
            Console.ReadKey();

            channel.BasicCancel(consumerTag);

            channel.Close();
            conn.Close();
        }
    }
}
