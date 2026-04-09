using System.Net;
using System.Text;
using Microsoft.Data.Sqlite;
using QadoPoolStack.Desktop.Configuration;
using QadoPoolStack.Desktop.Domain;
using QadoPoolStack.Desktop.Infrastructure.Logging;
using QadoPoolStack.Desktop.Persistence;
using QadoPoolStack.Desktop.Services.Accounts;
using QadoPoolStack.Desktop.Services.Node;

namespace QadoPoolStack.Desktop.Tests;

public sealed class DepositAttributionTests : IDisposable
{
    private readonly string _baseDirectory = Path.Combine(Path.GetTempPath(), "QadoPoolStack.Tests", Guid.NewGuid().ToString("N"));
    private readonly AppPaths _paths;

    public DepositAttributionTests()
    {
        _paths = new AppPaths(_baseDirectory);
        ResetDataDirectory();
    }

    [Fact]
    public async Task PollAsync_CreditsConfirmedPoolDepositToMatchingCustodianWalletOwner()
    {
        var repository = CreateRepository();
        await repository.InitializeAsync();

        var logger = CreateLogger();
        var poolAddress = new string('p', 64).Replace('p', 'a');
        var custodianAddress = new string('c', 64);
        var depositAddress = new string('d', 64);
        var createdUtc = new DateTimeOffset(2026, 4, 8, 16, 0, 0, TimeSpan.Zero);

        var user = new PoolUser(
            "user-1",
            "alice",
            "pwdhash",
            "pwdsalt",
            depositAddress,
            "protected-deposit-private-key",
            null,
            0,
            "0",
            createdUtc,
            custodianAddress,
            "protected-custodian-private-key");
        await repository.InsertUserAsync(user);

        var nodeHandler = new MutableNodeHandler();
        using var httpClient = new HttpClient(nodeHandler)
        {
            BaseAddress = new Uri("http://localhost")
        };
        var nodeClient = new QadoNodeClient(httpClient, logger);
        var settings = new PoolSettings
        {
            PoolMinerPublicKey = poolAddress,
            DepositMinConfirmations = 1
        };
        var monitor = new DepositMonitorService(repository, nodeClient, logger, settings);

        nodeHandler.ResponseFactory = request =>
        {
            if (request.RequestUri?.AbsolutePath == $"/v1/address/{poolAddress}/incoming")
            {
                return Json($$"""
                    {
                      "address": "{{poolAddress}}",
                      "tip_height": "9001",
                      "next_cursor": "",
                      "items": [
                        {
                          "event_id": "evt-1",
                          "txid": "tx-1",
                          "status": "confirmed",
                          "block_height": "9000",
                          "block_hash": "block-1",
                          "confirmations": "3",
                          "timestamp_utc": "2026-04-08T16:05:00Z",
                          "to_address": "{{poolAddress}}",
                          "amount_atomic": "125000000",
                          "from_address": "{{custodianAddress}}",
                          "from_addresses": [],
                          "tx_index": 0,
                          "transfer_index": 0
                        }
                      ]
                    }
                    """);
            }

            return NotFound();
        };

        await monitor.PollAsync();

        var balance = await repository.GetBalanceAsync(user.UserId);
        var ledgerEntries = await repository.ListLedgerEntriesByUserIdAsync(user.UserId);
        var depositEntry = Assert.Single(ledgerEntries, entry => entry.EntryType == LedgerEntryType.DepositCredit);

        Assert.NotNull(balance);
        Assert.Equal(125000000L, balance!.AvailableAtomic);
        Assert.Equal(125000000L, balance.TotalDepositedAtomic);
        Assert.Contains(custodianAddress, depositEntry.MetadataJson, StringComparison.Ordinal);
        Assert.Contains("\"matchedSenderPublicKey\":\"" + custodianAddress + "\"", depositEntry.MetadataJson, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PollAsync_TracksMatchingPoolDepositAsPendingUntilEnoughConfirmationsExist()
    {
        var repository = CreateRepository();
        await repository.InitializeAsync();

        var logger = CreateLogger();
        var poolAddress = new string('p', 64).Replace('p', 'a');
        var custodianAddress = new string('c', 64);
        var depositAddress = new string('d', 64);
        var createdUtc = new DateTimeOffset(2026, 4, 8, 16, 0, 0, TimeSpan.Zero);

        var user = new PoolUser(
            "user-2",
            "bob",
            "pwdhash",
            "pwdsalt",
            depositAddress,
            "protected-deposit-private-key",
            null,
            0,
            "0",
            createdUtc,
            custodianAddress,
            "protected-custodian-private-key");
        await repository.InsertUserAsync(user);

        var nodeHandler = new MutableNodeHandler();
        using var httpClient = new HttpClient(nodeHandler)
        {
            BaseAddress = new Uri("http://localhost")
        };
        var nodeClient = new QadoNodeClient(httpClient, logger);
        var settings = new PoolSettings
        {
            PoolMinerPublicKey = poolAddress,
            DepositMinConfirmations = 6
        };
        var monitor = new DepositMonitorService(repository, nodeClient, logger, settings);

        nodeHandler.ResponseFactory = request =>
        {
            if (request.RequestUri?.AbsolutePath == $"/v1/address/{poolAddress}/incoming")
            {
                return Json($$"""
                    {
                      "address": "{{poolAddress}}",
                      "tip_height": "9001",
                      "next_cursor": "",
                      "items": [
                        {
                          "event_id": "evt-pending-1",
                          "txid": "tx-pending-1",
                          "status": "pending",
                          "block_height": "0",
                          "block_hash": "",
                          "confirmations": "0",
                          "timestamp_utc": "2026-04-08T16:05:00Z",
                          "to_address": "{{poolAddress}}",
                          "amount_atomic": "250000000",
                          "from_address": "{{custodianAddress}}",
                          "from_addresses": [],
                          "tx_index": 0,
                          "transfer_index": 0
                        }
                      ]
                    }
                    """);
            }

            return NotFound();
        };

        await monitor.PollAsync();

        var balance = await repository.GetBalanceAsync(user.UserId);
        var pendingDeposits = await repository.GetPendingIncomingDepositAtomicForUserAsync(user.UserId, settings.DepositMinConfirmations);
        var ledgerEntries = await repository.ListLedgerEntriesByUserIdAsync(user.UserId);

        Assert.NotNull(balance);
        Assert.Equal(0, balance!.AvailableAtomic);
        Assert.Equal(0, balance.TotalDepositedAtomic);
        Assert.Equal(250000000L, pendingDeposits);
        Assert.DoesNotContain(ledgerEntries, entry => entry.EntryType == LedgerEntryType.DepositCredit);
    }

    public void Dispose()
    {
        ResetDataDirectory();
    }

    private PoolRepository CreateRepository()
        => new(new SqliteConnectionFactory(_paths));

    private PoolLogger CreateLogger()
        => new(Path.Combine(_baseDirectory, "logs", $"{Guid.NewGuid():N}.log"));

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

    private static HttpResponseMessage Json(string json)
    {
        return new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent(json, Encoding.UTF8, "application/json")
        };
    }

    private static HttpResponseMessage NotFound() => new(HttpStatusCode.NotFound);

    private sealed class MutableNodeHandler : HttpMessageHandler
    {
        public Func<HttpRequestMessage, HttpResponseMessage> ResponseFactory { get; set; } = _ => new HttpResponseMessage(HttpStatusCode.NotFound);

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
            => Task.FromResult(ResponseFactory(request));
    }
}
