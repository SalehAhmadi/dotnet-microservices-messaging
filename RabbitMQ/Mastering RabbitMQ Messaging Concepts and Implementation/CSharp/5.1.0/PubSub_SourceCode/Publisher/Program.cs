using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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

            while(true)
            {
                Console.Write("Enter message:");
                string message = Console.ReadLine();

                if (message == "exit")
                    break;

                channel.BasicPublish("ex.fanout", "", null, Encoding.UTF8.GetBytes(message));
            }

            channel.Close();
            conn.Close();
        }
    }
}
