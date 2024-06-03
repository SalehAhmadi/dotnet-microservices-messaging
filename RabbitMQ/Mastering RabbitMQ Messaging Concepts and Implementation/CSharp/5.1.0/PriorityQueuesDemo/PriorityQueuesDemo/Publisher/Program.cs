using System;
using System.Collections.Generic;
using System.Text;
using RabbitMQ.Client;

namespace Publisher
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

            //Create a "fanout" exchange
            channel.ExchangeDeclare(
               "ex.fanout",
               "fanout",
               true,
               false,
               null);

            //Create a queue that supports message priorities (with max message priority = 5)
            var queueArguments = new Dictionary<string, object>()
            {
                { "x-max-priority", 2 }
            };

            channel.QueueDeclare(
                "my.queue",
                true,
                false,
                false,
                queueArguments);

            //Bind the queue to the fanout exchange
            channel.QueueBind("my.queue", "ex.fanout", "");

            Console.WriteLine("Publisher is ready. Press a key to start sending messages.");
            Console.ReadKey();

            //Send sample messages with low (priority=1) and high (priority=2) priorities

            //Low priority messages
            sendMessage(channel, 1);
            sendMessage(channel, 1);
            sendMessage(channel, 1);

            //High priority messages
            sendMessage(channel, 2);
            sendMessage(channel, 2);

            Console.WriteLine("Press a key to exit.");
            Console.ReadKey();

            channel.QueueDelete("my.queue");
            channel.ExchangeDelete("ex.fanout");

            channel.Close();
            conn.Close();
        }

        private static void sendMessage(IModel channel, byte priority)
        {
            var basicProperties = channel.CreateBasicProperties();
            basicProperties.Priority = priority;

            string message = "Message with priority=" + priority;
            channel.BasicPublish("ex.fanout", "", basicProperties, Encoding.UTF8.GetBytes(message));

            Console.WriteLine("SENT:"  + message);
        }
    }
}
