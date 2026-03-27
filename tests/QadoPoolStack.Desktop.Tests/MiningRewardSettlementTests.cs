using System.Net;
using System.Net.Http.Headers;
using System.Text;
using Microsoft.Data.Sqlite;
using QadoPoolStack.Desktop.Configuration;
using QadoPoolStack.Desktop.Domain;
using QadoPoolStack.Desktop.Infrastructure.Logging;
using QadoPoolStack.Desktop.Persistence;
using QadoPoolStack.Desktop.Services.Mining;
using QadoPoolStack.Desktop.Services.Node;

namespace QadoPoolStack.Desktop.Tests;

public sealed class MiningRewardSettlementTests : IDisposable
{
    private readonly string _baseDirectory = Path.Combine(Path.GetTempPath(), "QadoPoolStack.Tests", Guid.NewGuid().ToString("N"));
    private readonly AppPaths _paths;

    public MiningRewardSettlementTests()
    {
        _paths = new AppPaths(_baseDirectory);
        ResetDataDirectory();
    }

    [Fact]
    public async Task CreditAcceptedBlockAsync_LeavesRewardAsImmatureUntilSettled()
    {
        var repository = CreateRepository();
        await repository.InitializeAsync();

        var logger = CreateLogger();
        var settings = new PoolSettings
        {
            PoolFeeBasisPoints = 0,
            MiningRewardMinConfirmations = 12
        };

        var context = await SeedSingleMinerRoundAsync(repository);
        var accounting = new RoundAccountingService(repository, logger);

        await accounting.CreditAcceptedBlockAsync(context.Job, context.Miner, context.BlockHashHex, context.Job.HeightText, settings);

        var balance = await repository.GetBalanceAsync(context.User.UserId);
        var immature = await repository.GetUserImmatureMiningAtomicAsync(context.User.UserId);
        var foundBlock = Assert.Single(await repository.ListRecentBlocksAsync(10));

        Assert.Equal(0, balance?.AvailableAtomic ?? 0);
        Assert.Equal(1_000L, immature);
        Assert.Equal(FoundBlockStatus.Pending, foundBlock.Status);
    }

    [Fact]
    public async Task FoundBlockSettlementService_FinalizesCanonicalRewardAndReversesCanonicalMismatch()
    {
        var repository = CreateRepository();
        await repository.InitializeAsync();

        var logger = CreateLogger();
        var settings = new PoolSettings
        {
            PoolFeeBasisPoints = 0,
            MiningRewardMinConfirmations = 12
        };

        var context = await SeedSingleMinerRoundAsync(repository);
        var accounting = new RoundAccountingService(repository, logger);

        await accounting.CreditAcceptedBlockAsync(context.Job, context.Miner, context.BlockHashHex, context.Job.HeightText, settings);

        var nodeHandler = new MutableNodeHandler();
        using var httpClient = new HttpClient(nodeHandler)
        {
            BaseAddress = new Uri("http://localhost")
        };
        var nodeClient = new QadoNodeClient(httpClient, logger);
        var settlement = new FoundBlockSettlementService(repository, nodeClient, accounting, logger, settings);

        nodeHandler.ResponseFactory = request =>
        {
            if (request.RequestUri?.AbsolutePath == "/v1/tip")
            {
                return Json("""
                    {"height":"111","hash":"tip-hash","timestamp_utc":"2026-03-27T12:00:00Z","chainwork":"0"}
                    """);
            }

            if (request.RequestUri?.AbsolutePath == "/v1/block/100")
            {
                return Json($$"""
                    {"hash":"{{context.BlockHashHex}}","height":"100","prev_hash":"prev-100","timestamp_utc":"2026-03-27T11:59:00Z","miner":"miner","tx_count":1}
                    """);
            }

            return NotFound();
        };

        await settlement.ReconcileAsync();

        var finalizedBalance = await repository.GetBalanceAsync(context.User.UserId);
        var finalizedImmature = await repository.GetUserImmatureMiningAtomicAsync(context.User.UserId);
        var finalizedBlock = Assert.Single(await repository.ListRecentBlocksAsync(10));

        Assert.Equal(1_000L, finalizedBalance?.AvailableAtomic ?? 0);
        Assert.Equal(1_000L, finalizedBalance?.TotalMinedAtomic ?? 0);
        Assert.Equal(0, finalizedImmature);
        Assert.Equal(FoundBlockStatus.Finalized, finalizedBlock.Status);

        nodeHandler.ResponseFactory = request =>
        {
            if (request.RequestUri?.AbsolutePath == "/v1/tip")
            {
                return Json("""
                    {"height":"112","hash":"tip-hash-2","timestamp_utc":"2026-03-27T12:10:00Z","chainwork":"0"}
                    """);
            }

            if (request.RequestUri?.AbsolutePath == "/v1/block/100")
            {
                return Json("""
                    {"hash":"different-canonical-hash","height":"100","prev_hash":"prev-100","timestamp_utc":"2026-03-27T11:59:00Z","miner":"miner","tx_count":1}
                    """);
            }

            return NotFound();
        };

        await settlement.ReconcileAsync();

        var reversedBalance = await repository.GetBalanceAsync(context.User.UserId);
        var reversedBlock = Assert.Single(await repository.ListRecentBlocksAsync(10));

        Assert.Equal(0, reversedBalance?.AvailableAtomic ?? 0);
        Assert.Equal(0, reversedBalance?.TotalMinedAtomic ?? 0);
        Assert.Equal(FoundBlockStatus.Reversed, reversedBlock.Status);
    }

    [Fact]
    public async Task FoundBlockSettlementService_BackfillsLegacyWonRoundAndReversesLegacyReward()
    {
        var repository = CreateRepository();
        await repository.InitializeAsync();

        var logger = CreateLogger();
        var settings = new PoolSettings
        {
            PoolFeeBasisPoints = 0,
            MiningRewardMinConfirmations = 12
        };

        var context = await SeedLegacyWonRoundWithRewardAsync(repository);

        var nodeHandler = new MutableNodeHandler();
        using var httpClient = new HttpClient(nodeHandler)
        {
            BaseAddress = new Uri("http://localhost")
        };
        var nodeClient = new QadoNodeClient(httpClient, logger);
        var accounting = new RoundAccountingService(repository, logger);
        var settlement = new FoundBlockSettlementService(repository, nodeClient, accounting, logger, settings);

        nodeHandler.ResponseFactory = request =>
        {
            if (request.RequestUri?.AbsolutePath == "/v1/tip")
            {
                return Json("""
                    {"height":"111","hash":"tip-hash","timestamp_utc":"2026-03-27T12:00:00Z","chainwork":"0"}
                    """);
            }

            if (request.RequestUri?.AbsolutePath == "/v1/block/100")
            {
                return Json("""
                    {"hash":"different-canonical-hash","height":"100","prev_hash":"prev-100","timestamp_utc":"2026-03-27T11:59:00Z","miner":"miner","tx_count":1}
                    """);
            }

            return NotFound();
        };

        await settlement.ReconcileAsync();

        var balance = await repository.GetBalanceAsync(context.User.UserId);
        var immature = await repository.GetUserImmatureMiningAtomicAsync(context.User.UserId);
        var foundBlock = Assert.Single(await repository.ListRecentBlocksAsync(10));

        Assert.Equal(0, balance?.AvailableAtomic ?? 0);
        Assert.Equal(0, balance?.TotalMinedAtomic ?? 0);
        Assert.Equal(0, immature);
        Assert.Equal(FoundBlockStatus.Reversed, foundBlock.Status);
    }

    [Fact]
    public async Task FoundBlockSettlementService_BackfillsLegacyRewardEvenWhenRoundIsNoLongerWon()
    {
        var repository = CreateRepository();
        await repository.InitializeAsync();

        var logger = CreateLogger();
        var settings = new PoolSettings
        {
            PoolFeeBasisPoints = 0,
            MiningRewardMinConfirmations = 12
        };

        var context = await SeedLegacyRewardOnClosedRoundAsync(repository);

        var nodeHandler = new MutableNodeHandler();
        using var httpClient = new HttpClient(nodeHandler)
        {
            BaseAddress = new Uri("http://localhost")
        };
        var nodeClient = new QadoNodeClient(httpClient, logger);
        var accounting = new RoundAccountingService(repository, logger);
        var settlement = new FoundBlockSettlementService(repository, nodeClient, accounting, logger, settings);

        nodeHandler.ResponseFactory = request =>
        {
            if (request.RequestUri?.AbsolutePath == "/v1/tip")
            {
                return Json("""
                    {"height":"111","hash":"tip-hash","timestamp_utc":"2026-03-27T12:00:00Z","chainwork":"0"}
                    """);
            }

            if (request.RequestUri?.AbsolutePath == "/v1/block/100")
            {
                return Json("""
                    {"hash":"different-canonical-hash","height":"100","prev_hash":"prev-100","timestamp_utc":"2026-03-27T11:59:00Z","miner":"miner","tx_count":1}
                    """);
            }

            return NotFound();
        };

        await settlement.ReconcileAsync();

        var balance = await repository.GetBalanceAsync(context.User.UserId);
        var immature = await repository.GetUserImmatureMiningAtomicAsync(context.User.UserId);
        var foundBlock = Assert.Single(await repository.ListRecentBlocksAsync(10));

        Assert.Equal(0, balance?.AvailableAtomic ?? 0);
        Assert.Equal(0, balance?.TotalMinedAtomic ?? 0);
        Assert.Equal(0, immature);
        Assert.Equal(FoundBlockStatus.Reversed, foundBlock.Status);
    }

    public void Dispose()
    {
        ResetDataDirectory();
    }

    private PoolRepository CreateRepository()
        => new(new SqliteConnectionFactory(_paths));

    private PoolLogger CreateLogger()
        => new(Path.Combine(_baseDirectory, "logs", $"{Guid.NewGuid():N}.log"));

    private async Task<MiningTestContext> SeedSingleMinerRoundAsync(PoolRepository repository)
    {
        var createdUtc = DateTimeOffset.UtcNow;
        var user = new PoolUser(
            "user-1",
            "alice",
            "pwdhash",
            "pwdsalt",
            "aa".PadLeft(64, 'a'),
            "secret",
            null,
            0,
            "0",
            createdUtc);
        await repository.InsertUserAsync(user);
        await repository.EnsureBalanceAsync(user.UserId);

        var miner = new MinerRecord(
            "miner-1",
            user.UserId,
            "bb".PadLeft(64, 'b'),
            1d,
            "tokenhash",
            "tokentext",
            true,
            createdUtc,
            createdUtc,
            createdUtc);
        await repository.UpsertMinerAsync(miner);

        var roundId = await repository.InsertRoundAsync(new PoolRound(
            0,
            "job-1",
            "100",
            "prev-100",
            "00".PadLeft(64, '0'),
            new string('0', 290),
            "1000",
            createdUtc,
            null,
            RoundStatus.Open,
            null));

        var job = new PoolJob(
            "pool-job-1",
            roundId,
            miner.MinerId,
            "node-job-1",
            "100",
            "prev-100",
            "00".PadLeft(64, '0'),
            "00".PadLeft(64, '0'),
            new string('0', 290),
            "123456789",
            "1000",
            createdUtc,
            createdUtc.AddMinutes(5),
            true);
        await repository.InsertJobAsync(job);

        var inserted = await repository.InsertShareAsync(new ShareRecord(
            0,
            roundId,
            job.PoolJobId,
            miner.MinerId,
            "1",
            "123456789",
            "share-hash-1",
            1d,
            ShareStatus.Accepted,
            false,
            createdUtc));
        Assert.True(inserted);

        return new MiningTestContext(user, miner, job, "pool-block-hash-1");
    }

    private async Task<MiningTestContext> SeedLegacyWonRoundWithRewardAsync(PoolRepository repository)
    {
        var context = await SeedSingleMinerRoundAsync(repository);
        var closedUtc = DateTimeOffset.UtcNow;

        await repository.ExecuteInTransactionAsync(async (connection, transaction) =>
        {
            await repository.UpdateRoundStateAsync(context.Job.RoundId, RoundStatus.Won, context.BlockHashHex, closedUtc, connection, transaction);
            await repository.UpdateBalanceAsync(context.User.UserId, 1_000L, 0, 1_000L, 0, 0, connection, transaction);
            await repository.InsertLedgerEntryAsync(
                new LedgerEntryRecord(
                    Guid.NewGuid().ToString("N"),
                    context.User.UserId,
                    LedgerEntryType.BlockReward,
                    1_000L,
                    $"round:{context.Job.RoundId}",
                    "{}",
                    closedUtc),
                connection,
                transaction);
        });

        return context;
    }

    private async Task<MiningTestContext> SeedLegacyRewardOnClosedRoundAsync(PoolRepository repository)
    {
        var context = await SeedSingleMinerRoundAsync(repository);
        var closedUtc = DateTimeOffset.UtcNow;

        await repository.ExecuteInTransactionAsync(async (connection, transaction) =>
        {
            await repository.UpdateRoundStateAsync(context.Job.RoundId, RoundStatus.Closed, context.BlockHashHex, closedUtc, connection, transaction);
            await repository.UpdateBalanceAsync(context.User.UserId, 1_000L, 0, 1_000L, 0, 0, connection, transaction);
            await repository.InsertLedgerEntryAsync(
                new LedgerEntryRecord(
                    Guid.NewGuid().ToString("N"),
                    context.User.UserId,
                    LedgerEntryType.BlockReward,
                    1_000L,
                    $"round:{context.Job.RoundId}",
                    "{}",
                    closedUtc),
                connection,
                transaction);
        });

        return context;
    }

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

    private sealed record MiningTestContext(PoolUser User, MinerRecord Miner, PoolJob Job, string BlockHashHex);

    private sealed class MutableNodeHandler : HttpMessageHandler
    {
        public Func<HttpRequestMessage, HttpResponseMessage> ResponseFactory { get; set; } = _ => new HttpResponseMessage(HttpStatusCode.NotFound);

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
            => Task.FromResult(ResponseFactory(request));
    }
}
