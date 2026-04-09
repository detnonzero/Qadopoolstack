using QadoPoolStack.Desktop.Configuration;

namespace QadoPoolStack.Desktop.Tests;

public sealed class PoolSettingsTests
{
    [Fact]
    public void Clone_CreatesIndependentCopy()
    {
        var settings = new PoolSettings
        {
            NodeBaseUrl = "http://127.0.0.1:18080",
            HttpPort = 8080,
            EnableHttps = true,
            PreferHttpsWhenAvailable = true,
            PoolMinerPublicKey = "abc",
            AuthRateLimitPerMinute = 20,
            UserApiRateLimitPerMinute = 120,
            MinerRequestRateLimitPerMinute = 90,
            ShareRateLimitPerMinute = 240,
            NewAccountCreditAmount = "1.25"
        };

        var clone = settings.Clone();
        clone.NodeBaseUrl = "http://localhost:19090";
        clone.AuthRateLimitPerMinute = 10;
        clone.ShareRateLimitPerMinute = 480;
        clone.NewAccountCreditAmount = "2.50";

        Assert.Equal("http://127.0.0.1:18080", settings.NodeBaseUrl);
        Assert.Equal(20, settings.AuthRateLimitPerMinute);
        Assert.True(settings.PreferHttpsWhenAvailable);
        Assert.Equal(240, settings.ShareRateLimitPerMinute);
        Assert.Equal("1.25", settings.NewAccountCreditAmount);
        Assert.False(settings.SemanticallyEquals(clone));
    }
}
