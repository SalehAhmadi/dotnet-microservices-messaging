using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace HeadersDemo
{
    class Program
    {
        static void Main(string[] args)
        {
            IConnection conn;
            IModel channel;

            ConnectionFactory factory = new ConnectionFactory();
            // "guest"/"guest" by default, limited to localhost connections
            factory.HostName = "localhost";
            factory.VirtualHost = "/";
            factory.Port = 5672;
            factory.UserName = "guest";
            factory.Password = "guest";

            conn = factory.CreateConnection();
            channel = conn.CreateModel();

            channel.ExchangeDeclare(
                "ex.headers",
                "headers",
                true,
                false,
                null);

            channel.QueueDeclare(
                "my.queue1",
                true,
                false,
                false,
                null);

            channel.QueueDeclare(
                "my.queue2",
                true,
                false,
                false,
                null);

            channel.QueueBind(
                "my.queue1",
                "ex.headers",
                "",
                new Dictionary<string, object>()
                {
                    {"x-match","all" },
                    {"job","convert" },
                    {"format","jpeg" }
                });

            channel.QueueBind(
                "my.queue2",
                "ex.headers",
                "",
                new Dictionary<string, object>()
                {
                    {"x-match","any" },
                    {"job","convert" },
                    {"format","jpeg" }
                });

            IBasicProperties props = channel.CreateBasicProperties();
            props.Headers = new Dictionary<string, object>();
            props.Headers.Add("job", "convert");
            props.Headers.Add("format", "jpeg");

            channel.BasicPublish(
                "ex.headers",
                "",
                props,
                Encoding.UTF8.GetBytes("Message 1"));

            props = channel.CreateBasicProperties();
            props.Headers = new Dictionary<string, object>();
            props.Headers.Add("job", "convert");
            props.Headers.Add("format", "bmp");

            channel.BasicPublish(
                "ex.headers",
                "",
                props,
                Encoding.UTF8.GetBytes("Message 2"));
        }
    }
}
