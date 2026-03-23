using QadoPoolStack.Desktop.Utilities;

namespace QadoPoolStack.Desktop.Tests;

public sealed class DifficultyFixedPointTests
{
    [Fact]
    public void ToScaled_ClampsValuesAboveSupportedRange()
    {
        var scaled = DifficultyFixedPoint.ToScaled(double.MaxValue);

        Assert.Equal(long.MaxValue, scaled);
    }

    [Fact]
    public void ToNormalizedDouble_ClampsToSupportedMaximum()
    {
        var normalized = DifficultyFixedPoint.ToNormalizedDouble(double.MaxValue);

        Assert.True(normalized <= DifficultyFixedPoint.MaxNormalizedDifficulty);
        Assert.True(normalized >= DifficultyFixedPoint.MaxNormalizedDifficulty - 1d);
    }
}
