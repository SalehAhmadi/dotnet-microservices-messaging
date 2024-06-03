using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace AlternateDemo
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

            //Enable channel level publisher confirms (to be able to get confirmations from RabbitMQ broker)
            channel.ConfirmSelect();

            channel.ExchangeDeclare(
                "ex.fanout",
                ExchangeType.Fanout,
                true,
                false,
                null);

            channel.ExchangeDeclare(
                "ex.direct",
                ExchangeType.Direct,
                true,
                false,
                new Dictionary<string, object>()
                {
                    { "alternate-exchange", "ex.fanout" }
                });

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

            channel.QueueDeclare(
                "my.unrouted",
                true,
                false,
                false,
                null);

            channel.QueueBind("my.queue1", "ex.direct", "video");
            channel.QueueBind("my.queue2", "ex.direct", "image");
            channel.QueueBind("my.unrouted", "ex.fanout", "");

            channel.BasicPublish(
                "ex.direct",
                "video",
                null,
                Encoding.UTF8.GetBytes("Message with routing key video"));

            channel.BasicPublish(
                "ex.direct",
                "text",
                null,
                Encoding.UTF8.GetBytes("Message with routing key text"));

            //Wait until all published messages are confirmed
            channel.WaitForConfirms();
        }
    }
}
