using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Newtonsoft.Json;
using System.Collections.Concurrent;
using Demo.Common;

namespace Requestor
{
    class Program
    {
        static void Main(string[] args)
        {
            ConcurrentDictionary<string, CalculationRequest> waitingRequests = new ConcurrentDictionary<string, CalculationRequest>();

            ConnectionFactory factory = new ConnectionFactory();
            // "guest"/"guest" by default, limited to localhost connections
            factory.HostName = "localhost";
            factory.VirtualHost = "/";
            factory.Port = 5672;
            factory.UserName = "guest";
            factory.Password = "guest";

            IConnection conn = factory.CreateConnection();
            IModel channel = conn.CreateModel();

            string responseQueueName = "res." + Guid.NewGuid().ToString();
            channel.QueueDeclare(responseQueueName);

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (sender, e) =>
              {
                  string requestId = Encoding.UTF8.GetString((byte[])e.BasicProperties.Headers[Constants.RequestIdHeaderKey]);
                  CalculationRequest request;

                  if (waitingRequests.TryRemove(requestId, out request))
                  {
                      string messageData = System.Text.Encoding.UTF8.GetString(e.Body);
                      CalculationResponse response = JsonConvert.DeserializeObject<CalculationResponse>(messageData);                      

                      Console.WriteLine("Calculation result: " + request.ToString() +"="+ response.ToString());
                  }
              };

            channel.BasicConsume(responseQueueName, true, consumer);

            Console.WriteLine("Press a key to send requests");
            Console.ReadKey();

            sendRequest(waitingRequests, channel, new CalculationRequest(2, 4, OperationType.Add), responseQueueName);
            sendRequest(waitingRequests, channel, new CalculationRequest(14, 6, OperationType.Subtract), responseQueueName);
            sendRequest(waitingRequests, channel, new CalculationRequest(50, 2, OperationType.Add), responseQueueName);            
            sendRequest(waitingRequests, channel, new CalculationRequest(30, 6, OperationType.Subtract), responseQueueName);

            Console.ReadKey();

            channel.Close();
            conn.Close();
        }

        private static void sendRequest(
            ConcurrentDictionary<string, CalculationRequest> waitingRequest, 
            IModel channel, CalculationRequest request, string responseQueueName)
        {
            string requestId = Guid.NewGuid().ToString();
            string requestData = JsonConvert.SerializeObject(request);

            waitingRequest[requestId] = request;

            var basicProperties = channel.CreateBasicProperties();
            basicProperties.Headers = new Dictionary<string, object>();
            basicProperties.Headers.Add(Constants.RequestIdHeaderKey, Encoding.UTF8.GetBytes(requestId));
            basicProperties.Headers.Add(Constants.ResponseQueueHeaderKey, Encoding.UTF8.GetBytes(responseQueueName));

            channel.BasicPublish(
                "", 
                "requests",
                basicProperties, 
                Encoding.UTF8.GetBytes(requestData));
        }
    }
}
