using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace PushPullDemo
{
    class Program
    {
        static IConnection conn;
        static IModel channel;

        static void Main(string[] args)
        {
            ConnectionFactory factory = new ConnectionFactory();
            // "guest"/"guest" by default, limited to localhost connections
            factory.HostName = "localhost";
            factory.VirtualHost = "/";
            factory.Port = 5672;
            factory.UserName = "guest";
            factory.Password = "guest";

            conn = factory.CreateConnection();
            channel = conn.CreateModel();

            //readMessagesWithPushModel();
            readMessagesWithPullModel();

            channel.Close();
            conn.Close();

        }

        private static void readMessagesWithPushModel()
        {
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (sender, e) =>
              {
                  string message = Encoding.UTF8.GetString(e.Body.ToArray());
                  Console.WriteLine("Message:"+message);
              };

            string consumerTag = channel.BasicConsume("my.queue1", true, consumer);

            Console.WriteLine("Subscribed. Press a key to unsubscribe and exit.");
            Console.ReadKey();

            channel.BasicCancel(consumerTag);
        }

        private static void readMessagesWithPullModel()
        {
            Console.WriteLine("Reading messages from queue. Press 'e' to exit.");

            while(true)
            {
                Console.WriteLine("Trying to get a message from the queue...");

                BasicGetResult result = channel.BasicGet("my.queue1", true);
                if(result != null)
                {
                    string message = Encoding.UTF8.GetString(result.Body.ToArray());
                    Console.WriteLine("Message:" + message);
                }

                if(Console.KeyAvailable)
                {
                    var keyInfo = Console.ReadKey();
                    if (keyInfo.KeyChar == 'e' || keyInfo.KeyChar == 'E')
                        return;
                }

                Thread.Sleep(2000);
            }
        }
    }
}
