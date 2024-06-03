using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Demo.Common;
using Newtonsoft.Json;
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
                string requestData = System.Text.Encoding.UTF8.GetString(e.Body);
                CalculationRequest request = JsonConvert.DeserializeObject<CalculationRequest>(requestData);
                Console.WriteLine("Request received:" + request.ToString());

                CalculationResponse response = new CalculationResponse();

                if(request.Operation == OperationType.Add)
                {
                    response.Result = request.Number1 + request.Number2;
                }
                else if(request.Operation == OperationType.Subtract)
                {
                    response.Result = request.Number1 - request.Number2;
                }                

                string responseData = JsonConvert.SerializeObject(response);

                var basicProperties = channel.CreateBasicProperties();
                basicProperties.Headers = new Dictionary<string, object>();
                basicProperties.Headers.Add(Constants.RequestIdHeaderKey, e.BasicProperties.Headers[Constants.RequestIdHeaderKey]);

                channel.BasicPublish(
                "",
                "responses",
                basicProperties,
                Encoding.UTF8.GetBytes(responseData));
            };

            channel.BasicConsume("requests", true, consumer);

            Console.WriteLine("Press a key to exit.");
            Console.ReadKey();

            channel.Close();
            conn.Close();
        }
    }
}
