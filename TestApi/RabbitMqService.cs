using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Channels;
using RabbitMQ.Client;

namespace TestApi;

public sealed class RabbitMqService : IRabbitMqService, IHostedService, IAsyncDisposable
{
    private const string ExchangeName = "task.events";
    private const string RoutingKey = "task.completed";
    private static readonly TimeSpan ShutdownTimeout = TimeSpan.FromSeconds(5);
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter() }
    };

    private readonly Channel<TaskCompletedEvent> _queue = Channel.CreateBounded<TaskCompletedEvent>(
        new BoundedChannelOptions(256)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false
        });
    private readonly ConnectionFactory _connectionFactory;
    private readonly ILogger<RabbitMqService> _logger;
    private readonly CancellationTokenSource _shutdown = new();
    private IConnection? _connection;
    private IChannel? _channel;
    private Task? _worker;

    public RabbitMqService(IConfiguration configuration, ILogger<RabbitMqService> logger)
    {
        _logger = logger;
        _connectionFactory = new ConnectionFactory
        {
            HostName = configuration["RabbitMq:HostName"] ?? "localhost",
            Port = int.TryParse(configuration["RabbitMq:Port"], out var port) ? port : 5672,
            UserName = configuration["RabbitMq:UserName"] ?? "guest",
            Password = configuration["RabbitMq:Password"] ?? "guest",
            VirtualHost = configuration["RabbitMq:VirtualHost"] ?? "/"
        };
    }

    public bool TryPublish(TaskCompletedEvent taskCompletedEvent)
    {
        if (_queue.Writer.TryWrite(taskCompletedEvent))
            return true;

        _logger.LogWarning("RabbitMQ event queue is full or stopping; completion event for task {TaskId} was not queued.", taskCompletedEvent.TaskId);
        return false;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _worker = ProcessQueueAsync(_shutdown.Token);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _queue.Writer.TryComplete();
        if (_worker is null)
            return;

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(ShutdownTimeout);

        try
        {
            await _worker.WaitAsync(timeout.Token);
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("Timed out while draining RabbitMQ events; stopping the publisher.");
            _shutdown.Cancel();
            try
            {
                await _worker;
            }
            catch (OperationCanceledException)
            {
            }
        }
    }

    private async Task ProcessQueueAsync(CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var taskCompletedEvent in _queue.Reader.ReadAllAsync(cancellationToken))
            {
                try
                {
                    await PublishAsync(taskCompletedEvent, cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception exception)
                {
                    _logger.LogError(exception, "Failed to publish completion event for task {TaskId} to RabbitMQ.", taskCompletedEvent.TaskId);
                }
            }
        }
        finally
        {
            await DisposeRabbitMqAsync();
        }
    }

    private async Task PublishAsync(TaskCompletedEvent taskCompletedEvent, CancellationToken cancellationToken)
    {
        try
        {
            if (_channel is null || !_channel.IsOpen)
                await ConnectAsync(cancellationToken);

            var body = JsonSerializer.SerializeToUtf8Bytes(taskCompletedEvent, JsonOptions);
            await _channel!.BasicPublishAsync(ExchangeName, RoutingKey, body, cancellationToken);
        }
        catch
        {
            await DisposeRabbitMqAsync();
            throw;
        }
    }

    private async Task ConnectAsync(CancellationToken cancellationToken)
    {
        var connection = await _connectionFactory.CreateConnectionAsync(cancellationToken);
        try
        {
            var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);
            await channel.ExchangeDeclareAsync(
                ExchangeName,
                ExchangeType.Topic,
                durable: true,
                autoDelete: false,
                cancellationToken: cancellationToken);
            _connection = connection;
            _channel = channel;
        }
        catch
        {
            await connection.DisposeAsync();
            throw;
        }
    }

    private async ValueTask DisposeRabbitMqAsync()
    {
        var channel = _channel;
        var connection = _connection;
        _channel = null;
        _connection = null;

        if (channel is not null)
            await channel.DisposeAsync();
        if (connection is not null)
            await connection.DisposeAsync();
    }

    public async ValueTask DisposeAsync()
    {
        _shutdown.Cancel();
        _queue.Writer.TryComplete();
        if (_worker is not null)
        {
            try
            {
                await _worker;
            }
            catch (OperationCanceledException)
            {
            }
        }

        _shutdown.Dispose();
    }
}
