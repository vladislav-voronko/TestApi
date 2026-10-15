using System.Collections.Concurrent;
using System.Net;
using System.Net.Http.Json;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Xunit;

namespace TestApi.Tests;

public sealed class TaskCompletionIntegrationTests
{
    [Fact]
    public async Task CreatingAndCompletingTask_PublishesCompletionEvent()
    {
        await using var factory = new TestApiFactory();
        using var client = factory.CreateClient();

        var createResponse = await client.PostAsJsonAsync("/tasks", new CreateTaskRequest
        {
            Title = "Buy milk",
            Priority = Priority.High
        });

        Assert.Equal(HttpStatusCode.Created, createResponse.StatusCode);
        var createdTask = await createResponse.Content.ReadFromJsonAsync<TaskItem>();
        Assert.NotNull(createdTask);
        Assert.False(createdTask.IsCompleted);
        Assert.Null(createdTask.CompletedAt);

        var completeResponse = await client.PutAsync($"/tasks/{createdTask.Id}/complete", content: null);

        Assert.Equal(HttpStatusCode.OK, completeResponse.StatusCode);
        var completedTask = await completeResponse.Content.ReadFromJsonAsync<TaskItem>();
        Assert.NotNull(completedTask);
        Assert.True(completedTask.IsCompleted);
        Assert.NotNull(completedTask.CompletedAt);

        var publishedEvent = Assert.Single(factory.Publisher.Events);
        Assert.Equal(createdTask.Id, publishedEvent.TaskId);
        Assert.Equal("Buy milk", publishedEvent.Title);
        Assert.Equal(Priority.High, publishedEvent.Priority);
        Assert.Equal(completedTask.CompletedAt, publishedEvent.CompletedAt);
    }

    private sealed class TestApiFactory : WebApplicationFactory<Program>
    {
        private readonly string _databaseName = $"tasks-tests-{Guid.NewGuid()}";

        public RecordingRabbitMqService Publisher { get; } = new();

        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            builder.ConfigureServices(services =>
            {
                services.RemoveAll<IDbContextOptionsConfiguration<TaskDbContext>>();
                services.RemoveAll<DbContextOptions<TaskDbContext>>();
                services.RemoveAll<DbContextOptions>();
                services.RemoveAll<TaskDbContext>();
                services.AddDbContext<TaskDbContext>(options =>
                    options.UseInMemoryDatabase(_databaseName));

                services.RemoveAll<IRabbitMqService>();
                services.AddSingleton<IRabbitMqService>(Publisher);
            });
        }
    }

    private sealed class RecordingRabbitMqService : IRabbitMqService
    {
        private readonly ConcurrentQueue<TaskCompletedEvent> _events = new();

        public IReadOnlyCollection<TaskCompletedEvent> Events => _events.ToArray();

        public bool TryPublish(TaskCompletedEvent taskCompletedEvent)
        {
            _events.Enqueue(taskCompletedEvent);
            return true;
        }
    }
}
