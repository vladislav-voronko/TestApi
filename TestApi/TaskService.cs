using Microsoft.EntityFrameworkCore;

namespace TestApi
{
    public class TaskService
    {
        private readonly TaskDbContext _dbContext;
        private readonly IRabbitMqService _eventPublisher;

        public TaskService(TaskDbContext dbContext, IRabbitMqService eventPublisher)
        {
            _dbContext = dbContext;
            _eventPublisher = eventPublisher;
        }
        public async Task<List<TaskItem>> GetTasksAsync()
        {
            return await _dbContext.Tasks.AsNoTracking().ToListAsync();
        }

        public async Task<TaskItem> CreateTaskAsync(CreateTaskRequest request)
        {
            var task = new TaskItem
            {
                Id = Guid.NewGuid(),
                Title = request.Title,
                IsCompleted = false,
                CreatedAt = DateTimeOffset.UtcNow,
                Priority = request.Priority
            };

            _dbContext.Tasks.Add(task);
            await _dbContext.SaveChangesAsync();
            return task;
        }

        public async Task<TaskItem?> CompleteTaskAsync(Guid id)
        {
            var task = await _dbContext.Tasks.SingleOrDefaultAsync(task => task.Id == id);
            if (task is null)
                return null;

            if (task.IsCompleted)
                throw new TaskCompletionConflictException();

            task.IsCompleted = true;
            task.CompletedAt = DateTimeOffset.UtcNow;

            try
            {
                await _dbContext.SaveChangesAsync();
            }
            catch (DbUpdateConcurrencyException exception)
            {
                throw new TaskCompletionConflictException(exception);
            }

            _eventPublisher.TryPublish(new TaskCompletedEvent(
                task.Id,
                task.Title,
                task.CompletedAt.Value,
                task.Priority));

            return task;
        }

        public async Task<bool> DeleteAsync(Guid id)
        {
            var task = await _dbContext.Tasks.FindAsync(id);
            if (task == null)
                return false;

            _dbContext.Tasks.Remove(task);
            await _dbContext.SaveChangesAsync();
            return true;
        }

    }

    public sealed class TaskCompletionConflictException : Exception
    {
        public TaskCompletionConflictException() : base("The task has already been completed.")
        {
        }

        public TaskCompletionConflictException(Exception innerException)
            : base("The task was completed by another request.", innerException)
        {
        }
    }
}
