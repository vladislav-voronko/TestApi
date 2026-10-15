namespace TestApi;

public sealed record TaskCompletedEvent(
    Guid TaskId,
    string Title,
    DateTimeOffset CompletedAt,
    Priority Priority);

public interface IRabbitMqService
{
    bool TryPublish(TaskCompletedEvent taskCompletedEvent);
}
