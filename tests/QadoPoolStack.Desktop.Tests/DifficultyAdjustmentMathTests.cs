using QadoPoolStack.Desktop.Domain;
using QadoPoolStack.Desktop.Services.Mining;

namespace QadoPoolStack.Desktop.Tests;

public sealed class DifficultyAdjustmentMathTests
{
    [Fact]
    public void ComputeAverageShareSeconds_UsesFullObservationWindowForSingleShare()
    {
        var sampleSince = new DateTimeOffset(2026, 3, 21, 0, 0, 0, TimeSpan.Zero);
        var now = sampleSince.AddMinutes(10);
        var shares = new[]
        {
            CreateShare(sampleSince.AddMinutes(9))
        };

        var averageSeconds = DifficultyAdjustmentMath.ComputeAverageShareSeconds(shares, sampleSince, now);

        Assert.Equal(600d, averageSeconds);
    }

    [Fact]
    public void ComputeAverageShareSeconds_UsesIntervalsWhenMultipleSharesExist()
    {
        var sampleSince = new DateTimeOffset(2026, 3, 21, 0, 0, 0, TimeSpan.Zero);
        var now = sampleSince.AddMinutes(10);
        var shares = new[]
        {
            CreateShare(sampleSince.AddSeconds(20)),
            CreateShare(sampleSince.AddSeconds(80)),
            CreateShare(sampleSince.AddSeconds(140))
        };

        var averageSeconds = DifficultyAdjustmentMath.ComputeAverageShareSeconds(shares, sampleSince, now);

        Assert.Equal(60d, averageSeconds);
    }

    private static ShareRecord CreateShare(DateTimeOffset submittedUtc)
        => new(1, 1, "job", "miner", "1", "1", "hash", 32d, ShareStatus.Accepted, false, submittedUtc);
}
