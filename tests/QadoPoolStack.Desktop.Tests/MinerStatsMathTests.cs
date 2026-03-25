using System.Numerics;
using QadoPoolStack.Desktop.Domain;
using QadoPoolStack.Desktop.Services.Mining;
using QadoPoolStack.Desktop.Utilities;

namespace QadoPoolStack.Desktop.Tests;

public sealed class MinerStatsMathTests
{
    [Fact]
    public void ComputeApproximateNetworkHashrate_RequiresWindowPlusAnchorRounds()
    {
        var rounds = new[]
        {
            CreateRound(2, 200d, new DateTimeOffset(2026, 3, 25, 12, 0, 0, TimeSpan.Zero)),
            CreateRound(1, 100d, new DateTimeOffset(2026, 3, 25, 11, 59, 0, TimeSpan.Zero))
        };

        var hashrate = DifficultyAdjustmentMath.ComputeApproximateNetworkHashrate(rounds, 2);

        Assert.Equal(0d, hashrate);
    }

    [Fact]
    public void ComputeApproximateNetworkHashrate_UsesPerRoundTargetsAndIntervals()
    {
        var now = new DateTimeOffset(2026, 3, 25, 12, 0, 0, TimeSpan.Zero);
        var rounds = new[]
        {
            CreateRound(3, 200d, now),
            CreateRound(2, 100d, now.AddSeconds(-10)),
            CreateRound(1, 50d, now.AddSeconds(-30))
        };

        var hashrate = DifficultyAdjustmentMath.ComputeApproximateNetworkHashrate(rounds, 2);

        Assert.InRange(hashrate, 9.99d, 10.01d);
    }

    [Fact]
    public void ComputeApproximateNetworkHashrate_ClampsNonPositiveIntervalsToOneSecond()
    {
        var now = new DateTimeOffset(2026, 3, 25, 12, 0, 0, TimeSpan.Zero);
        var rounds = new[]
        {
            CreateRound(3, 1d, now),
            CreateRound(2, 10d, now),
            CreateRound(1, 2d, now.AddSeconds(-10))
        };

        var hashrate = DifficultyAdjustmentMath.ComputeApproximateNetworkHashrate(rounds, 2);

        Assert.InRange(hashrate, 0.99d, 1.01d);
    }

    private static PoolRound CreateRound(long roundId, double expectedHashesPerBlock, DateTimeOffset openedUtc)
        => new(roundId, $"job-{roundId}", roundId.ToString(), $"prev-{roundId}", CreateTargetHex(expectedHashesPerBlock), "", "0", openedUtc, null, RoundStatus.Open, null);

    private static string CreateTargetHex(double expectedHashesPerBlock)
    {
        var work = new BigInteger(expectedHashesPerBlock);
        return UInt256Utility.ToFixedHex(((BigInteger.One << 256) - BigInteger.One) / work);
    }
}
