using QadoPoolStack.Desktop.Configuration;
using QadoPoolStack.Desktop.Utilities;

namespace QadoPoolStack.Desktop.Tests;

public sealed class DifficultyCalibrationTests
{
    [Fact]
    public void GetRawDifficultyPerUnit_UsesTargetShareMidpoint()
    {
        var settings = new PoolSettings
        {
            ShareTargetSecondsMin = 5,
            ShareTargetSecondsMax = 10
        };

        var rawPerUnit = DifficultyCalibration.GetRawDifficultyPerUnit(settings);

        Assert.Equal(75_000_000d, rawPerUnit);
    }

    [Fact]
    public void Conversion_RoundTripsBetweenCalibratedAndRawDifficulty()
    {
        var settings = new PoolSettings
        {
            ShareTargetSecondsMin = 5,
            ShareTargetSecondsMax = 10
        };

        var rawDifficulty = DifficultyCalibration.ToRawDifficulty(1d, settings);
        var calibratedDifficulty = DifficultyCalibration.ToCalibratedDifficulty(rawDifficulty, settings);

        Assert.Equal(75_000_000d, rawDifficulty);
        Assert.Equal(1d, calibratedDifficulty);
    }
}
