using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace TopicDemo
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
                "ex.topic",
                "topic",
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

            channel.QueueDeclare(
                "my.queue3",
                true,
                false,
                false,
                null);

            channel.QueueBind("my.queue1", "ex.topic", "*.image.*");
            channel.QueueBind("my.queue2", "ex.topic", "#.image");
            channel.QueueBind("my.queue3", "ex.topic", "image.#");

            channel.BasicPublish(
                "ex.topic",
                "convert.image.bmp",
                null,
                Encoding.UTF8.GetBytes("Routing key is convert.image.bmp"));

            channel.BasicPublish(
                "ex.topic",
                "convert.bitmap.image",
                null,
                Encoding.UTF8.GetBytes("Routing key is convert.bitmap.image"));

            channel.BasicPublish(
                "ex.topic",
                "image.bitmap.32bit",
                null,
                Encoding.UTF8.GetBytes("Routing key is image.bitmap.32bit"));
        }
    }
}
