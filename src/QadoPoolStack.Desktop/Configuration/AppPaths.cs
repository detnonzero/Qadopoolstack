using System.IO;
using System.Reflection;

namespace QadoPoolStack.Desktop.Configuration;

public sealed class AppPaths
{
    public AppPaths(string? baseDirectory = null)
    {
        baseDirectory ??= AppContext.BaseDirectory;

        BaseDirectory = baseDirectory;
        DataDirectory = Path.Combine(baseDirectory, "data");
        CertificatesDirectory = Path.Combine(DataDirectory, "certs");
        SettingsFilePath = Path.Combine(DataDirectory, "poolsettings.json");
        DatabasePath = Path.Combine(DataDirectory, "pool.db");
        LogFilePath = Path.Combine(DataDirectory, "pool.log");
        WebRootPath = Path.Combine(baseDirectory, "wwwroot");
        AppVersion = Assembly.GetExecutingAssembly().GetName().Version?.ToString() ?? "0.1.0";
    }

    public string AppVersion { get; }

    public string BaseDirectory { get; }

    public string DataDirectory { get; }

    public string CertificatesDirectory { get; }

    public string SettingsFilePath { get; }

    public string DatabasePath { get; }

    public string LogFilePath { get; }

    public string WebRootPath { get; }

    public void EnsureDirectories()
    {
        Directory.CreateDirectory(DataDirectory);
        Directory.CreateDirectory(CertificatesDirectory);
        Directory.CreateDirectory(WebRootPath);
    }
}
