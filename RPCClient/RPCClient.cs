using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Collections.Concurrent;
using System.Text;

public class RPCClient : IDisposable
{
    private const string QUEUE_NAME = "rpc_queue";

    private readonly IConnection _connection;
    private readonly IModel _channel;
    private readonly string _replayQueueName;
    private readonly ConcurrentDictionary<string, TaskCompletionSource<string>> _callbackMapper = new();

    public RPCClient()
    {
        var factory = new ConnectionFactory() { HostName = "localhost" };
        _connection = factory.CreateConnection();
        _channel = _connection.CreateModel();
        // declare a server-named queue.
        _replayQueueName = _channel.QueueDeclare().QueueName;

        var consumer = new EventingBasicConsumer(_channel);
        consumer.Received += (model, ea) =>
        {
            // Attepmts to remove and return the value that has a specified key.
            // It returns true, if it has been successfully removed, otherwise false.
            if (!_callbackMapper.TryRemove(ea.BasicProperties.CorrelationId, out var tcs))
                return;

            var body = ea.Body.ToArray();
            var response = Encoding.UTF8.GetString(body);

            // Attempts to transition the underlying 'Taks' into the 'TaskStatus.RanToCompletion' state.
            // TaskStatus.RanToCompletion - The task completed execution successfully.
            tcs.TrySetResult(response);
        };

        _channel.BasicConsume(
            consumer: consumer,
            queue: _replayQueueName,
            autoAck: true);
    }

    public Task<string> CallAsync(string message, CancellationToken cancellationToken = default)
    {
        // Construct a completely empty content header for use with the Basic content class.
        var props = _channel.CreateBasicProperties();

        var correlationId = Guid.NewGuid().ToString();
        props.CorrelationId = correlationId;
        props.ReplyTo = _replayQueueName;

        var messageBytes = Encoding.UTF8.GetBytes(message);
        var tcs = new TaskCompletionSource<string>();
        _callbackMapper.TryAdd(correlationId, tcs);

        _channel.BasicPublish(
            exchange: string.Empty,
            routingKey: QUEUE_NAME,
            basicProperties: props,
            body: messageBytes);

        cancellationToken.Register(() => _callbackMapper.TryRemove(correlationId, out _));
        return tcs.Task;
    }

    public class Rpc
    {
        public static async Task Main(string[] args)
        {
            Console.WriteLine("RPC Client");
            var n = args.Length > 0 ? args[0] : "30";
            await InvokeAsync(n);

            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }

        private static async Task InvokeAsync(string n)
        {
            using var rpcClient = new RPCClient();

            Console.WriteLine($" [x] Requesting fib({n})");
            var response = await rpcClient.CallAsync(n);
            Console.WriteLine($" [.] Got '{response}'");
        }
    }



    public void Dispose()
    {
        _connection.Close();
    }
}