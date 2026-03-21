using QadoPoolStack.Desktop.Configuration;
using System;
using System.IO;

namespace QadoPoolStack.Desktop.Hosting;

internal static class PoolAccessPreference
{
    public static string BuildSummary(PoolSettings settings)
    {
        var httpUrl = BuildHttpUrl(settings);
        if (!IsHttpsAvailable(settings))
        {
            return settings.PreferHttpsWhenAvailable
                ? $"HTTPS preferred when available. Current endpoint: {httpUrl}"
                : $"HTTP endpoint: {httpUrl}";
        }

        var httpsUrl = BuildHttpsUrl(settings);
        return settings.PreferHttpsWhenAvailable
            ? $"Preferred endpoint: {httpsUrl} (HTTP fallback: {httpUrl})"
            : $"Preferred endpoint: {httpUrl} (HTTPS also available: {httpsUrl})";
    }

    public static string GetPreferredUrl(PoolSettings settings)
        => settings.PreferHttpsWhenAvailable && IsHttpsAvailable(settings)
            ? BuildHttpsUrl(settings)
            : BuildHttpUrl(settings);

    public static bool IsHttpsAvailable(PoolSettings settings)
        => settings.EnableHttps &&
           !string.IsNullOrWhiteSpace(settings.CertificatePath) &&
           File.Exists(settings.CertificatePath);

    private static string BuildHttpUrl(PoolSettings settings)
        => BuildUrl("http", GetPublicHost(settings), settings.HttpPort);

    private static string BuildHttpsUrl(PoolSettings settings)
        => BuildUrl("https", GetPublicHost(settings), settings.HttpsPort);

    private static string GetPublicHost(PoolSettings settings)
        => string.IsNullOrWhiteSpace(settings.DomainName) ? "localhost" : settings.DomainName.Trim();

    private static string BuildUrl(string scheme, string host, int port)
    {
        var builder = new UriBuilder(scheme, host)
        {
            Port = IsDefaultPort(scheme, port) ? -1 : port
        };

        return builder.Uri.GetLeftPart(UriPartial.Authority);
    }

    private static bool IsDefaultPort(string scheme, int port)
        => string.Equals(scheme, "http", StringComparison.OrdinalIgnoreCase) && port == 80 ||
           string.Equals(scheme, "https", StringComparison.OrdinalIgnoreCase) && port == 443;
}
