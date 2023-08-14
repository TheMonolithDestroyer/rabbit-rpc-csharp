using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

var factory = new ConnectionFactory() { HostName = "localhost" };
using var connection = factory.CreateConnection();

// Create a AMQP channel for communication with nodes.
// It has most of the operations(protocol methods, APIs).
using var channel = connection.CreateModel();

// Declare a queue.
channel.QueueDeclare(
    queue: "rpc_queue",
    durable: false,
    exclusive: false,
    autoDelete: false,
    arguments: null);

// Makes it possible to limit the number of unacknowledged messages on a channel when consuming.
// More correctly, how many messages can consumer take from the queue.
channel.BasicQos(
    prefetchSize: 0,
    prefetchCount: 1,
    global: false);

// declare consumer.
// consumer implementation built around C# event handlers.
var consumer = new EventingBasicConsumer(channel);
// Start a consumer
channel.BasicConsume(
    queue: "rpc_queue",
    autoAck: false,
    consumer: consumer);
Console.WriteLine($" [x] Awaiting RPC requests");

// create event handler.
consumer.Received += (model, ea) =>
{
    var response = string.Empty;

    var body = ea.Body.ToArray();
    var props = ea.BasicProperties;
    var replyProps = channel.CreateBasicProperties();
    replyProps.CorrelationId = props.CorrelationId;

    try
    {
        var message = Encoding.UTF8.GetString(body);
        var n = int.Parse(message);

        Console.WriteLine($" [.] Fib({n})");

        response = Fib(n).ToString();
    }
    catch (Exception ex)
    {
        Console.WriteLine($" [.] {ex.Message}");
        response = string.Empty;
    }
    finally
    {
        var responseBytes = Encoding.UTF8.GetBytes(response);

        //Publishes a message.
        channel.BasicPublish(
            exchange: string.Empty,
            routingKey: props.ReplyTo,
            basicProperties: replyProps,
            body: responseBytes);

        // Acknowledges one or more delivered messages.
        channel.BasicAck(
            deliveryTag: ea.DeliveryTag,
            multiple: false);
    }
};

Console.WriteLine(" Press [enter] to exit.");
Console.ReadLine();


// Assumes only valid positive integer input.
// Don't expect this one to work for big numbers, and it's probably the slowest recursive implementation possible.
static int Fib(int n)
{
    if (n is 0 or 1)
        return n;

    return Fib(n - 1) + Fib(n - 2);
}