using QadoPoolStack.Desktop.Configuration;
using QadoPoolStack.Desktop.Hosting;

namespace QadoPoolStack.Desktop.Tests;

public sealed class PoolAccessPreferenceTests : IDisposable
{
    private readonly string _tempDirectory = Path.Combine(Path.GetTempPath(), "QadoPoolStack.Tests.Access", Guid.NewGuid().ToString("N"));

    [Fact]
    public void BuildSummary_PrefersHttpsWhenAvailable()
    {
        Directory.CreateDirectory(_tempDirectory);
        var certificatePath = Path.Combine(_tempDirectory, "pool.pfx");
        File.WriteAllText(certificatePath, "stub");

        var settings = new PoolSettings
        {
            HttpPort = 80,
            HttpsPort = 443,
            EnableHttps = true,
            PreferHttpsWhenAvailable = true,
            DomainName = "pool.example.test",
            CertificatePath = certificatePath
        };

        var summary = PoolAccessPreference.BuildSummary(settings);
        var preferredUrl = PoolAccessPreference.GetPreferredUrl(settings);

        Assert.Equal("https://pool.example.test", preferredUrl);
        Assert.Contains("Preferred endpoint: https://pool.example.test", summary, StringComparison.Ordinal);
        Assert.Contains("HTTP fallback: http://pool.example.test", summary, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildSummary_FallsBackToHttpWhenHttpsUnavailable()
    {
        var settings = new PoolSettings
        {
            HttpPort = 8080,
            HttpsPort = 8443,
            EnableHttps = true,
            PreferHttpsWhenAvailable = true,
            DomainName = "pool.example.test",
            CertificatePath = Path.Combine(_tempDirectory, "missing-cert.pfx")
        };

        var summary = PoolAccessPreference.BuildSummary(settings);
        var preferredUrl = PoolAccessPreference.GetPreferredUrl(settings);

        Assert.Equal("http://pool.example.test:8080", preferredUrl);
        Assert.Contains("HTTPS preferred when available. Current endpoint: http://pool.example.test:8080", summary, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildSummary_PrefersHttpWhenPreferenceDisabled()
    {
        Directory.CreateDirectory(_tempDirectory);
        var certificatePath = Path.Combine(_tempDirectory, "pool-http-first.pfx");
        File.WriteAllText(certificatePath, "stub");

        var settings = new PoolSettings
        {
            HttpPort = 8080,
            HttpsPort = 8443,
            EnableHttps = true,
            PreferHttpsWhenAvailable = false,
            CertificatePath = certificatePath
        };

        var summary = PoolAccessPreference.BuildSummary(settings);
        var preferredUrl = PoolAccessPreference.GetPreferredUrl(settings);

        Assert.Equal("http://localhost:8080", preferredUrl);
        Assert.Contains("Preferred endpoint: http://localhost:8080", summary, StringComparison.Ordinal);
        Assert.Contains("HTTPS also available: https://localhost:8443", summary, StringComparison.Ordinal);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDirectory))
        {
            Directory.Delete(_tempDirectory, recursive: true);
        }
    }
}
