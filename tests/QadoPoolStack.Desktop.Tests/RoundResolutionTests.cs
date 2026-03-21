using Microsoft.Data.Sqlite;
using QadoPoolStack.Desktop.Configuration;
using QadoPoolStack.Desktop.Domain;
using QadoPoolStack.Desktop.Persistence;

namespace QadoPoolStack.Desktop.Tests;

public sealed class RoundResolutionTests : IDisposable
{
    private readonly string _baseDirectory = Path.Combine(Path.GetTempPath(), "QadoPoolStack.Tests", Guid.NewGuid().ToString("N"));
    private readonly AppPaths _paths;

    public RoundResolutionTests()
    {
        _paths = new AppPaths(_baseDirectory);
        ResetDataDirectory();
    }

    [Fact]
    public async Task ResolveOpenRoundAsync_CreatesNewRoundAndClosesOlderOpenRounds()
    {
        var repository = CreateRepository();
        await repository.InitializeAsync();

        await repository.InsertRoundAsync(CreateOpenRound("job-old", "100", "prev-old"));

        var resolved = await repository.ResolveOpenRoundAsync(
            "job-new",
            "101",
            "prev-new",
            "target-new",
            CreateHeaderHex(),
            "5000000000");

        var rounds = await repository.ListRecentRoundsAsync(10);

        Assert.Equal("prev-new", resolved.PrevHashHex);
        Assert.Single(rounds, round => round.Status == RoundStatus.Open);
        Assert.Contains(rounds, round => round.RoundId == resolved.RoundId && round.Status == RoundStatus.Open);
        Assert.Contains(rounds, round => round.PrevHashHex == "prev-old" && round.Status == RoundStatus.Closed);
    }

    [Fact]
    public async Task ResolveOpenRoundAsync_ReusesNewestMatchingRoundAndClosesDuplicates()
    {
        var repository = CreateRepository();
        await repository.InitializeAsync();

        await repository.InsertRoundAsync(CreateOpenRound("job-old", "100", "prev-match"));
        var latestMatchingRoundId = await repository.InsertRoundAsync(CreateOpenRound("job-latest", "101", "prev-match"));
        await repository.InsertRoundAsync(CreateOpenRound("job-other", "102", "prev-other"));

        var resolved = await repository.ResolveOpenRoundAsync(
            "job-refresh",
            "103",
            "prev-match",
            "target-match",
            CreateHeaderHex(),
            "6000000000");

        var rounds = await repository.ListRecentRoundsAsync(10);
        var openRounds = rounds.Where(round => round.Status == RoundStatus.Open).ToList();

        Assert.Equal(latestMatchingRoundId, resolved.RoundId);
        Assert.Single(openRounds);
        Assert.Equal(latestMatchingRoundId, openRounds[0].RoundId);
        Assert.DoesNotContain(rounds, round => round.RoundId != latestMatchingRoundId && round.Status == RoundStatus.Open);
    }

    public void Dispose()
    {
        ResetDataDirectory();
    }

    private PoolRepository CreateRepository()
        => new(new SqliteConnectionFactory(_paths));

    private void ResetDataDirectory()
    {
        SqliteConnection.ClearAllPools();

        if (Directory.Exists(_baseDirectory))
        {
            const int maxAttempts = 5;

            for (var attempt = 1; attempt <= maxAttempts; attempt++)
            {
                try
                {
                    Directory.Delete(_baseDirectory, recursive: true);
                    break;
                }
                catch (IOException) when (attempt < maxAttempts)
                {
                    Thread.Sleep(50 * attempt);
                }
                catch (UnauthorizedAccessException) when (attempt < maxAttempts)
                {
                    Thread.Sleep(50 * attempt);
                }
            }
        }
    }

    private static PoolRound CreateOpenRound(string nodeJobId, string heightText, string prevHashHex)
        => new(
            0,
            nodeJobId,
            heightText,
            prevHashHex,
            "00".PadLeft(64, '0'),
            CreateHeaderHex(),
            "4000000000",
            DateTimeOffset.UtcNow,
            null,
            RoundStatus.Open,
            null);

    private static string CreateHeaderHex()
        => new string('0', 290);
}
