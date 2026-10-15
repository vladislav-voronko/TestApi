using System.ComponentModel.DataAnnotations;

namespace TestApi
{
    public class TaskItem
    {
        public Guid Id { get; set; }
        public string Title { get; set; }
        public bool IsCompleted { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
        public DateTimeOffset? CompletedAt { get; set; }
        public Priority Priority { get; set; }
    }

    public enum Priority
    {
        Low, Medium, High
    }

    public class CreateTaskRequest
    {
        [Required]
        [StringLength(200, MinimumLength = 1)]
        public string Title { get; set; } = string.Empty;
        public Priority Priority { get; set; } = Priority.Medium;
    }

}
