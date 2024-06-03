using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Replier
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

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (sender, e) =>
            {
                string request = System.Text.Encoding.UTF8.GetString(e.Body.ToArray());
                Console.WriteLine("Request received:" + request);

                string response = "Response for " + request;

                channel.BasicPublish("", "responses", null, Encoding.UTF8.GetBytes(response));
            };

            channel.BasicConsume("requests", true, consumer);

            Console.WriteLine("Press a key to exit.");
            Console.ReadKey();

            channel.Close();
            conn.Close();
        }
    }
}
