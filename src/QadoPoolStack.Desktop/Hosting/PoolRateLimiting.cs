using Microsoft.AspNetCore.Http;
using System;
using System.Threading.RateLimiting;

namespace QadoPoolStack.Desktop.Hosting;

internal static class PoolRateLimiting
{
    public const string PublicAuthPolicy = "public-auth";
    public const string UserApiPolicy = "user-api";
    public const string MinerApiPolicy = "miner-api";
    public const string ShareSubmitPolicy = "share-submit";

    public static string GetIpPartitionKey(HttpContext httpContext)
    {
        var remoteIp = httpContext.Connection.RemoteIpAddress?.ToString();
        return string.IsNullOrWhiteSpace(remoteIp) ? "ip:unknown" : $"ip:{remoteIp}";
    }

    public static string GetBearerOrIpPartitionKey(HttpContext httpContext, string prefix)
    {
        var bearerToken = GetBearerToken(httpContext);
        if (!string.IsNullOrWhiteSpace(bearerToken))
        {
            return $"{prefix}:bearer:{bearerToken}";
        }

        return $"{prefix}:{GetIpPartitionKey(httpContext)}";
    }

    public static string GetMinerTokenOrIpPartitionKey(HttpContext httpContext, string prefix)
    {
        var bearerToken = GetBearerToken(httpContext);
        if (!string.IsNullOrWhiteSpace(bearerToken))
        {
            return $"{prefix}:bearer:{bearerToken}";
        }

        var minerToken = httpContext.Request.Headers["X-Miner-Token"].ToString().Trim();
        if (!string.IsNullOrWhiteSpace(minerToken))
        {
            return $"{prefix}:miner:{minerToken}";
        }

        return $"{prefix}:{GetIpPartitionKey(httpContext)}";
    }

    public static FixedWindowRateLimiterOptions CreateFixedWindowOptions(int permitLimit)
    {
        return new FixedWindowRateLimiterOptions
        {
            PermitLimit = Math.Max(1, permitLimit),
            Window = TimeSpan.FromMinutes(1),
            QueueLimit = 0,
            AutoReplenishment = true
        };
    }

    public static TokenBucketRateLimiterOptions CreateShareSubmitOptions(int permitLimitPerMinute)
    {
        var normalizedLimit = Math.Max(1, permitLimitPerMinute);
        var tokensPerPeriod = Math.Max(1, (int)Math.Ceiling(normalizedLimit / 12d));
        return new TokenBucketRateLimiterOptions
        {
            TokenLimit = normalizedLimit,
            QueueLimit = 0,
            TokensPerPeriod = tokensPerPeriod,
            ReplenishmentPeriod = TimeSpan.FromSeconds(5),
            AutoReplenishment = true
        };
    }

    private static string? GetBearerToken(HttpContext httpContext)
    {
        var authorization = httpContext.Request.Headers.Authorization.ToString();
        return authorization.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase)
            ? authorization["Bearer ".Length..].Trim()
            : null;
    }
}
