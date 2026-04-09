using System.Net;
using System.Text;
using Microsoft.Data.Sqlite;
using QadoPoolStack.Desktop.Configuration;
using QadoPoolStack.Desktop.Domain;
using QadoPoolStack.Desktop.Infrastructure.Logging;
using QadoPoolStack.Desktop.Infrastructure.Security;
using QadoPoolStack.Desktop.Persistence;
using QadoPoolStack.Desktop.Services.Accounts;
using QadoPoolStack.Desktop.Services.Node;

namespace QadoPoolStack.Desktop.Tests;

public sealed class RegistrationCreditTests : IDisposable
{
    private readonly string _baseDirectory = Path.Combine(Path.GetTempPath(), "QadoPoolStack.Tests", Guid.NewGuid().ToString("N"));
    private readonly AppPaths _paths;

    public RegistrationCreditTests()
    {
        _paths = new AppPaths(_baseDirectory);
        ResetDataDirectory();
    }

    [Fact]
    public async Task RegisterAsync_CreditsConfiguredAmountWhenPoolDeltaIsSufficient()
    {
        var repository = CreateRepository();
        await repository.InitializeAsync();

        var poolAddress = new string('a', 64);
        var settings = new PoolSettings
        {
            PoolMinerPublicKey = poolAddress,
            NewAccountCreditAmount = "2.5"
        };

        var service = CreateAccountsService(repository, poolAddress, "10000000000");

        var result = await service.RegisterAsync("alice", "very-secure-password", settings);
        var balance = await repository.GetBalanceAsync(result.User.UserId);
        var entries = await repository.ListLedgerEntriesByUserIdAsync(result.User.UserId);

        Assert.NotNull(balance);
        Assert.Equal(2_500_000_000L, balance!.AvailableAtomic);
        var creditEntry = Assert.Single(entries, entry => entry.Reference == "signup-credit");
        Assert.Equal(LedgerEntryType.ManualAdjustment, creditEntry.EntryType);
        Assert.Equal(2_500_000_000L, creditEntry.DeltaAtomic);
    }

    [Fact]
    public async Task RegisterAsync_SkipsConfiguredAmountWhenPoolDeltaIsInsufficient()
    {
        var repository = CreateRepository();
        await repository.InitializeAsync();

        var poolAddress = new string('b', 64);
        var settings = new PoolSettings
        {
            PoolMinerPublicKey = poolAddress,
            NewAccountCreditAmount = "3.0"
        };

        var existingUser = new PoolUser(
            "existing-user",
            "funded",
            "pwdhash",
            "pwdsalt",
            new string('c', 64),
            "protected-deposit-private-key",
            null,
            0,
            "0",
            DateTimeOffset.UtcNow);

        await repository.ExecuteInTransactionAsync(async (connection, transaction) =>
        {
            await repository.InsertUserAsync(existingUser, connection, transaction);
            await repository.EnsureBalanceAsync(existingUser.UserId, connection, transaction);
            await repository.UpdateBalanceAsync(existingUser.UserId, 1_500_000_000L, 0, 0, 0, 0, connection, transaction);
        });

        var service = CreateAccountsService(repository, poolAddress, "4000000000");

        var result = await service.RegisterAsync("bob", "very-secure-password", settings);
        var balance = await repository.GetBalanceAsync(result.User.UserId);
        var entries = await repository.ListLedgerEntriesByUserIdAsync(result.User.UserId);

        Assert.NotNull(balance);
        Assert.Equal(0L, balance!.AvailableAtomic);
        Assert.DoesNotContain(entries, entry => entry.Reference == "signup-credit");
    }

    public void Dispose()
    {
        ResetDataDirectory();
    }

    private PoolRepository CreateRepository()
        => new(new SqliteConnectionFactory(_paths));

    private UserAccountService CreateAccountsService(PoolRepository repository, string poolAddress, string balanceAtomic)
    {
        var logger = new PoolLogger(Path.Combine(_baseDirectory, "logs", $"{Guid.NewGuid():N}.log"));
        var secretProtector = new SecretProtector();
        var passwordHasher = new PasswordHasher();
        var sessionService = new SessionService(repository);
        var handler = new MutableNodeHandler(poolAddress, balanceAtomic);
        var client = new HttpClient(handler)
        {
            BaseAddress = new Uri("http://localhost")
        };
        var nodeClient = new QadoNodeClient(client, logger);
        return new UserAccountService(repository, passwordHasher, secretProtector, sessionService, nodeClient, logger);
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

    private sealed class MutableNodeHandler(string poolAddress, string balanceAtomic) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (request.RequestUri?.AbsolutePath == $"/v1/address/{poolAddress}")
            {
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent($$"""
                    {
                      "address": "{{poolAddress}}",
                      "balance_atomic": "{{balanceAtomic}}",
                      "nonce": "0",
                      "pending_outgoing_count": 0,
                      "pending_incoming_count": 0,
                      "latest_observed_height": "0"
                    }
                    """, Encoding.UTF8, "application/json")
                });
            }

            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.NotFound));
        }
    }
}
