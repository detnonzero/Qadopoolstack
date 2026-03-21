using System.Collections.Concurrent;
using System.IO;

namespace QadoPoolStack.Desktop.Infrastructure.Logging;

public sealed record PoolLogEntry(DateTimeOffset TimestampUtc, string Level, string Category, string Message);

public sealed class PoolLogger
{
    private readonly string _logFilePath;
    private readonly ConcurrentQueue<PoolLogEntry> _recentEntries = new();
    private readonly object _fileSync = new();

    public PoolLogger(string logFilePath)
    {
        _logFilePath = logFilePath;
    }

    public event Action<PoolLogEntry>? EntryWritten;

    public IReadOnlyCollection<PoolLogEntry> RecentEntries => _recentEntries.ToArray();

    public void Info(string category, string message) => Write("INFO", category, message);

    public void Warn(string category, string message) => Write("WARN", category, message);

    public void Error(string category, string message) => Write("ERROR", category, message);

    public void Error(string category, Exception exception, string? message = null)
    {
        var text = message is null ? exception.ToString() : $"{message}{Environment.NewLine}{exception}";
        Write("ERROR", category, text);
    }

    private void Write(string level, string category, string message)
    {
        var entry = new PoolLogEntry(DateTimeOffset.UtcNow, level, category, message);
        _recentEntries.Enqueue(entry);

        while (_recentEntries.Count > 300 && _recentEntries.TryDequeue(out _))
        {
        }

        var line = $"{entry.TimestampUtc:O} [{entry.Level}] [{entry.Category}] {entry.Message}{Environment.NewLine}";
        lock (_fileSync)
        {
            Directory.CreateDirectory(Path.GetDirectoryName(_logFilePath)!);
            File.AppendAllText(_logFilePath, line);
        }

        EntryWritten?.Invoke(entry);
    }
}
