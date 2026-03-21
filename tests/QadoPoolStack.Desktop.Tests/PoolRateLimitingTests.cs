using Microsoft.AspNetCore.Http;
using QadoPoolStack.Desktop.Hosting;
using System.Net;

namespace QadoPoolStack.Desktop.Tests;

public sealed class PoolRateLimitingTests
{
    [Fact]
    public void GetIpPartitionKey_UsesRemoteIpAddress()
    {
        var httpContext = new DefaultHttpContext();
        httpContext.Connection.RemoteIpAddress = IPAddress.Parse("203.0.113.7");

        var key = PoolRateLimiting.GetIpPartitionKey(httpContext);

        Assert.Equal("ip:203.0.113.7", key);
    }

    [Fact]
    public void GetBearerOrIpPartitionKey_PrefersBearerToken()
    {
        var httpContext = new DefaultHttpContext();
        httpContext.Connection.RemoteIpAddress = IPAddress.Parse("203.0.113.7");
        httpContext.Request.Headers.Authorization = "Bearer session-123";

        var key = PoolRateLimiting.GetBearerOrIpPartitionKey(httpContext, "user");

        Assert.Equal("user:bearer:session-123", key);
    }

    [Fact]
    public void GetMinerTokenOrIpPartitionKey_UsesMinerTokenBeforeIpFallback()
    {
        var httpContext = new DefaultHttpContext();
        httpContext.Connection.RemoteIpAddress = IPAddress.Parse("203.0.113.7");
        httpContext.Request.Headers["X-Miner-Token"] = "miner-456";

        var key = PoolRateLimiting.GetMinerTokenOrIpPartitionKey(httpContext, "miner");

        Assert.Equal("miner:miner:miner-456", key);
    }

    [Fact]
    public void CreateFixedWindowOptions_ClampsPermitLimitToPositiveValue()
    {
        var options = PoolRateLimiting.CreateFixedWindowOptions(0);

        Assert.Equal(1, options.PermitLimit);
        Assert.Equal(TimeSpan.FromMinutes(1), options.Window);
        Assert.Equal(0, options.QueueLimit);
        Assert.True(options.AutoReplenishment);
    }
}
