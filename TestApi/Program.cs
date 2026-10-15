using Microsoft.EntityFrameworkCore;
using TestApi;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddScoped<TaskService>();
builder.Services.AddSingleton<RabbitMqService>();
builder.Services.AddSingleton<IRabbitMqService>(services => services.GetRequiredService<RabbitMqService>());
builder.Services.AddHostedService(services => services.GetRequiredService<RabbitMqService>());

var connectionString = builder.Configuration.GetConnectionString("DefaultConnection")
                ?? throw new InvalidOperationException("Connection string 'DefaultConnection' was not found.");

builder.Services.AddDbContext<TaskDbContext>(options =>
                options.UseNpgsql(connectionString, npgsql =>
                        npgsql.MigrationsAssembly(typeof(TaskDbContext).Assembly.FullName)
                            .MigrationsHistoryTable("__tasks_ef_migrations_history"))
                    .UseSnakeCaseNamingConvention());


var app = builder.Build();

app.MapGet("/", () => "Hello World!");
app.MapPost("/tasks", async (CreateTaskRequest? request, TaskService taskService) =>
{
    if (request is null || string.IsNullOrWhiteSpace(request.Title) || request.Title.Length > 200)
        return Results.BadRequest("Task title must contain between 1 and 200 characters.");

    if (!Enum.IsDefined(request.Priority))
        return Results.BadRequest("Task priority is invalid.");

    var task = await taskService.CreateTaskAsync(request);
    return Results.Created("/tasks", task);
});

app.MapGet("/tasks", async (TaskService taskService) =>
    Results.Ok(await taskService.GetTasksAsync()));

app.MapPut("/tasks/{id:guid}/complete", async (Guid id, TaskService taskService) =>
{
    try
    {
        var task = await taskService.CompleteTaskAsync(id);
        return task is null ? Results.NotFound() : Results.Ok(task);
    }
    catch (TaskCompletionConflictException)
    {
        return Results.Conflict();
    }
});

app.MapDelete("/tasks/{id:guid}", async (Guid id, TaskService taskService) =>
{
    await taskService.DeleteAsync(id);
    return Results.NoContent();
});

app.Run();

public partial class Program { }
