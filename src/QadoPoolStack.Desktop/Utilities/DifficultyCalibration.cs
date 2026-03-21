using QadoPoolStack.Desktop.Configuration;

namespace QadoPoolStack.Desktop.Utilities;

public static class DifficultyCalibration
{
    public const double BaselineHashratePerDifficulty = 10_000_000d;

    public static double ToRawDifficulty(double calibratedDifficulty, PoolSettings settings)
    {
        var normalized = DifficultyFixedPoint.ToNormalizedDouble(calibratedDifficulty);
        return normalized * GetRawDifficultyPerUnit(settings);
    }

    public static double ToCalibratedDifficulty(double rawDifficulty, PoolSettings settings)
    {
        var normalized = DifficultyFixedPoint.ToNormalizedDouble(rawDifficulty);
        return normalized / GetRawDifficultyPerUnit(settings);
    }

    public static double GetRawDifficultyPerUnit(PoolSettings settings)
    {
        var targetShareSeconds = Math.Max(1d, (settings.ShareTargetSecondsMin + settings.ShareTargetSecondsMax) / 2d);
        return BaselineHashratePerDifficulty * targetShareSeconds;
    }
}
