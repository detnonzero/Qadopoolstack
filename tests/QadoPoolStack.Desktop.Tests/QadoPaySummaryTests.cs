using Microsoft.Data.Sqlite;
using QadoPoolStack.Desktop.Configuration;
using QadoPoolStack.Desktop.Domain;
using QadoPoolStack.Desktop.Persistence;
using QadoPoolStack.Desktop.Services.Accounts;

namespace QadoPoolStack.Desktop.Tests;

public sealed class QadoPaySummaryTests : IDisposable
{
    private readonly string _baseDirectory = Path.Combine(Path.GetTempPath(), "QadoPoolStack.Tests", Guid.NewGuid().ToString("N"));
    private readonly AppPaths _paths;

    public QadoPaySummaryTests()
    {
        _paths = new AppPaths(_baseDirectory);
        ResetDataDirectory();
    }

    [Fact]
    public async Task GetSnapshotAsync_ReturnsLocalDayTotalsAndLatestPayment()
    {
        var repository = CreateRepository();
        await repository.InitializeAsync();

        var localZone = TimeZoneInfo.Local;
        var nowLocal = TimeZoneInfo.ConvertTime(DateTimeOffset.UtcNow, localZone);
        var today = DateOnly.FromDateTime(nowLocal.DateTime);
        var yesterday = today.AddDays(-1);

        var user = new PoolUser(
            "user-1",
            "alice",
            "pwdhash",
            "pwdsalt",
            new string('d', 64),
            "protected-deposit-private-key",
            null,
            0,
            "0",
            DateTimeOffset.UtcNow);

        await repository.ExecuteInTransactionAsync(async (connection, transaction) =>
        {
            await repository.InsertUserAsync(user, connection, transaction);

            await repository.InsertLedgerEntryAsync(
                new LedgerEntryRecord(
                    Guid.NewGuid().ToString("N"),
                    user.UserId,
                    LedgerEntryType.InternalTransferOut,
                    -2_000_000_000L,
                    "transfer:oldfriend",
                    """{"to":"oldfriend","note":"yesterday"}""",
                    CreateLocalTimestamp(yesterday, 22, 10, localZone)),
                connection,
                transaction);

            await repository.InsertLedgerEntryAsync(
                new LedgerEntryRecord(
                    Guid.NewGuid().ToString("N"),
                    user.UserId,
                    LedgerEntryType.InternalTransferOut,
                    -1_500_000_000L,
                    "transfer:bob",
                    """{"to":"bob","note":"coffee"}""",
                    CreateLocalTimestamp(today, 9, 15, localZone)),
                connection,
                transaction);

            await repository.InsertLedgerEntryAsync(
                new LedgerEntryRecord(
                    Guid.NewGuid().ToString("N"),
                    user.UserId,
                    LedgerEntryType.InternalTransferIn,
                    700_000_000L,
                    "transfer:carol",
                    """{"from":"carol","note":"split"}""",
                    CreateLocalTimestamp(today, 11, 30, localZone)),
                connection,
                transaction);

            await repository.InsertLedgerEntryAsync(
                new LedgerEntryRecord(
                    Guid.NewGuid().ToString("N"),
                    user.UserId,
                    LedgerEntryType.InternalTransferOut,
                    -250_000_000L,
                    "transfer:dave",
                    """{"to":"dave","note":"last"}""",
                    CreateLocalTimestamp(today, 18, 45, localZone)),
                connection,
                transaction);
        });

        var service = new QadoPayService(repository);
        var snapshot = await service.GetSnapshotAsync(user);

        Assert.Equal(1_750_000_000L, snapshot.Overview.SentTodayAtomic);
        Assert.Equal(700_000_000L, snapshot.Overview.ReceivedTodayAtomic);
        Assert.NotNull(snapshot.Overview.LastPayment);
        Assert.Equal(LedgerEntryType.InternalTransferOut, snapshot.Overview.LastPayment!.EntryType);
        Assert.Contains("\"to\":\"dave\"", snapshot.Overview.LastPayment.MetadataJson, StringComparison.Ordinal);
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

    private static DateTimeOffset CreateLocalTimestamp(DateOnly date, int hour, int minute, TimeZoneInfo zone)
    {
        var localTime = date.ToDateTime(new TimeOnly(hour, minute), DateTimeKind.Unspecified);
        return new DateTimeOffset(localTime, zone.GetUtcOffset(localTime));
    }
}
