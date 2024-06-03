using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace WorkerDemo
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

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (sender, e) =>
              {
                  string message = System.Text.Encoding.UTF8.GetString(e.Body);
                  Console.WriteLine($"[{workerName}] Message:{message}");
              };

            var consumerTag = channel.BasicConsume("my.queue1", true, consumer);

            Console.WriteLine("Waiting for messages. Press a key to exit.");
            Console.ReadKey();

            channel.Close();
            conn.Close();
        }
    }
}
