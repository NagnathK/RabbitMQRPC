namespace RabbitMQ.RPCClient
{
    using RabbitMQ.Client;
    using RabbitMQ.Client.Events;
    using System;
    using System.Collections.Concurrent;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    public class RpcClient
    {
        private const string QUEUE_NAME = "rpc_queue";

        private readonly IConnection connection;
        private readonly IModel channel;
        private readonly string replyQueueName;
        private readonly EventingBasicConsumer consumer;
        private readonly ConcurrentDictionary<string, TaskCompletionSource<string>> callbackMapper =
                    new ConcurrentDictionary<string, TaskCompletionSource<string>>();

        public RpcClient()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };

            connection = factory.CreateConnection();

            channel = connection.CreateModel();

            replyQueueName = channel.QueueDeclare().QueueName;

            consumer = new EventingBasicConsumer(channel);

            consumer.Received += (model, ea) =>
            {
                if (!callbackMapper.TryRemove(ea.BasicProperties.CorrelationId, out TaskCompletionSource<string> tcs))
                {
                    return;
                }

                var body = ea.Body.ToArray();
                var response = Encoding.UTF8.GetString(body);
                tcs.TrySetResult(response);
            };
        }

        public Task<string> CallAsync(string message, CancellationToken cancellationToken = default(CancellationToken))
        {
            IBasicProperties props = channel.CreateBasicProperties();

            var correlationId = Guid.NewGuid().ToString();

            props.CorrelationId = correlationId;

            props.ReplyTo = replyQueueName;

            var messageBytes = Encoding.UTF8.GetBytes(message);

            var tcs = new TaskCompletionSource<string>();

            callbackMapper.TryAdd(correlationId, tcs);

            // Publish message and sit back until we get response back
            channel.BasicPublish(
                exchange: "",
                routingKey: QUEUE_NAME,
                basicProperties: props,
                body: messageBytes);

            channel.BasicConsume(
                consumer: consumer,
                queue: replyQueueName,
                autoAck: true);

            // For every response message the client checks if the CorrelationId
            cancellationToken.Register(() => callbackMapper.TryRemove(correlationId, out var tmp));

            // Return response/result back to user
            return tcs.Task;
        }

        public void Close()
        {
            connection.Close();
        }

    }
}
