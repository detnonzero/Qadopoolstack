using System.Globalization;
using System.Security.Cryptography;
using System.Text.Json;
using QadoPoolStack.Desktop.Configuration;
using QadoPoolStack.Desktop.Domain;
using QadoPoolStack.Desktop.Infrastructure.Logging;
using QadoPoolStack.Desktop.Infrastructure.Security;
using QadoPoolStack.Desktop.Persistence;
using QadoPoolStack.Desktop.Services.Node;
using QadoPoolStack.Desktop.Utilities;

namespace QadoPoolStack.Desktop.Services.Accounts;

public sealed class PasswordHasher
{
    private const int SaltSize = 16;
    private const int HashSize = 32;
    private const int Iterations = 210_000;

    public (string saltHex, string hashHex) Hash(string password)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(password);

        var salt = RandomNumberGenerator.GetBytes(SaltSize);
        var hash = Rfc2898DeriveBytes.Pbkdf2(password, salt, Iterations, HashAlgorithmName.SHA512, HashSize);
        return (HexUtility.ToLowerHex(salt), HexUtility.ToLowerHex(hash));
    }

    public bool Verify(string password, string saltHex, string hashHex)
    {
        var salt = HexUtility.Parse(saltHex, SaltSize);
        var expectedHash = HexUtility.Parse(hashHex, HashSize);
        var actualHash = Rfc2898DeriveBytes.Pbkdf2(password, salt, Iterations, HashAlgorithmName.SHA512, HashSize);
        return CryptographicOperations.FixedTimeEquals(expectedHash, actualHash);
    }
}

public sealed record LoginResult(PoolUser User, string SessionToken);

public sealed record MinerBindingResult(MinerRecord Miner, string ApiToken);

public sealed class SessionService
{
    private readonly PoolRepository _repository;

    public SessionService(PoolRepository repository)
    {
        _repository = repository;
    }

    public async Task<string> CreateSessionAsync(string userId, PoolSettings settings, CancellationToken cancellationToken = default)
    {
        var rawToken = HexUtility.CreateTokenHex(32);
        var session = new UserSession(
            Guid.NewGuid().ToString("N"),
            userId,
            HexUtility.HashSha256Hex(rawToken),
            DateTimeOffset.UtcNow,
            DateTimeOffset.UtcNow.AddHours(settings.SessionLifetimeHours),
            DateTimeOffset.UtcNow);

        await _repository.InsertSessionAsync(session, cancellationToken).ConfigureAwait(false);
        return rawToken;
    }

    public async Task<PoolUser?> GetUserFromTokenAsync(string? rawToken, PoolSettings settings, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(rawToken))
        {
            return null;
        }

        var session = await _repository.GetSessionByTokenHashAsync(HexUtility.HashSha256Hex(rawToken), cancellationToken).ConfigureAwait(false);
        if (session is null)
        {
            return null;
        }

        if (session.ExpiresUtc <= DateTimeOffset.UtcNow)
        {
            await _repository.DeleteSessionAsync(session.SessionId, cancellationToken).ConfigureAwait(false);
            return null;
        }

        var newExpiry = DateTimeOffset.UtcNow.AddHours(settings.SessionLifetimeHours);
        await _repository.TouchSessionAsync(session.SessionId, DateTimeOffset.UtcNow, newExpiry, cancellationToken).ConfigureAwait(false);
        return await _repository.GetUserByIdAsync(session.UserId, cancellationToken).ConfigureAwait(false);
    }
}

public sealed class UserAccountService
{
    private readonly PoolRepository _repository;
    private readonly PasswordHasher _passwordHasher;
    private readonly SecretProtector _secretProtector;
    private readonly SessionService _sessionService;
    private readonly QadoNodeClient _nodeClient;
    private readonly PoolLogger _logger;
    private readonly SemaphoreSlim _registrationGate = new(1, 1);

    public UserAccountService(
        PoolRepository repository,
        PasswordHasher passwordHasher,
        SecretProtector secretProtector,
        SessionService sessionService,
        QadoNodeClient nodeClient,
        PoolLogger logger)
    {
        _repository = repository;
        _passwordHasher = passwordHasher;
        _secretProtector = secretProtector;
        _sessionService = sessionService;
        _nodeClient = nodeClient;
        _logger = logger;
    }

    public async Task<LoginResult> RegisterAsync(string username, string password, PoolSettings settings, CancellationToken cancellationToken = default)
    {
        if (!settings.AccountRegistrationEnabled)
        {
            throw new InvalidOperationException("Account creation is currently inactive.");
        }

        username = NormalizeUsername(username);
        ValidatePassword(password);

        await _registrationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (await _repository.GetUserByUsernameAsync(username, cancellationToken).ConfigureAwait(false) is not null)
            {
                throw new InvalidOperationException("Username already exists.");
            }

            var (passwordSaltHex, passwordHashHex) = _passwordHasher.Hash(password);
            var (depositPrivateKey, depositPublicKey) = KeyUtility.GenerateEd25519KeypairHex();

            var user = new PoolUser(
                Guid.NewGuid().ToString("N"),
                username,
                passwordHashHex,
                passwordSaltHex,
                depositPublicKey,
                _secretProtector.Protect(depositPrivateKey),
                null,
                0,
                "0",
                DateTimeOffset.UtcNow);

            var signupCreditAtomic = GetConfiguredSignupCreditAtomic(settings);
            var poolOnChainBalanceAtomic = await TryGetPoolOnChainBalanceAsync(settings, cancellationToken).ConfigureAwait(false);

            await _repository.ExecuteInTransactionAsync(async (connection, transaction) =>
            {
                await _repository.InsertUserAsync(user, connection, transaction, cancellationToken).ConfigureAwait(false);
                await _repository.EnsureBalanceAsync(user.UserId, connection, transaction, cancellationToken).ConfigureAwait(false);

                if (signupCreditAtomic <= 0 || poolOnChainBalanceAtomic is null)
                {
                    return;
                }

                var trackedSpendable = await _repository.GetTotalTrackedBalanceAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
                var immatureMining = await _repository.GetTotalImmatureMiningObligationAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
                var trackedObligations = checked(trackedSpendable + immatureMining);
                var poolBalanceDelta = checked(poolOnChainBalanceAtomic.Value - trackedObligations);
                if (poolBalanceDelta < signupCreditAtomic)
                {
                    _logger.Info("Auth", $"Skipped new account credit for user={user.Username}; delta={AmountUtility.FormatAtomic(poolBalanceDelta)} QADO is below required {AmountUtility.FormatAtomic(signupCreditAtomic)} QADO.");
                    return;
                }

                var now = DateTimeOffset.UtcNow;
                var configuredAmount = AmountUtility.FormatAtomic(signupCreditAtomic);
                await _repository.UpdateBalanceAsync(user.UserId, signupCreditAtomic, 0, 0, 0, 0, connection, transaction, cancellationToken).ConfigureAwait(false);
                await _repository.InsertLedgerEntryAsync(
                    new LedgerEntryRecord(
                        Guid.NewGuid().ToString("N"),
                        user.UserId,
                        LedgerEntryType.ManualAdjustment,
                        signupCreditAtomic,
                        "signup-credit",
                        JsonSerializer.Serialize(new
                        {
                            reason = "new_account_credit",
                            amount = configuredAmount,
                            source = "pool_balance_delta"
                        }),
                        now),
                    connection,
                    transaction,
                    cancellationToken).ConfigureAwait(false);
                _logger.Info("Auth", $"Granted new account credit {configuredAmount} QADO to user={user.Username} using pool balance delta {AmountUtility.FormatAtomic(poolBalanceDelta)} QADO.");
            }, cancellationToken).ConfigureAwait(false);

            var sessionToken = await _sessionService.CreateSessionAsync(user.UserId, settings, cancellationToken).ConfigureAwait(false);
            return new LoginResult(user, sessionToken);
        }
        finally
        {
            _registrationGate.Release();
        }
    }

    public async Task<LoginResult> LoginAsync(string username, string password, PoolSettings settings, CancellationToken cancellationToken = default)
    {
        username = NormalizeUsername(username);
        var user = await _repository.GetUserByUsernameAsync(username, cancellationToken).ConfigureAwait(false)
            ?? throw new InvalidOperationException("Invalid username or password.");

        if (!_passwordHasher.Verify(password, user.PasswordSaltHex, user.PasswordHashHex))
        {
            throw new InvalidOperationException("Invalid username or password.");
        }

        var sessionToken = await _sessionService.CreateSessionAsync(user.UserId, settings, cancellationToken).ConfigureAwait(false);
        return new LoginResult(user, sessionToken);
    }

    public Task<List<UserBalanceView>> ListUsersAsync(CancellationToken cancellationToken = default)
        => _repository.ListUsersWithBalancesAsync(cancellationToken);

    public Task<BalanceRecord?> GetBalanceAsync(string userId, CancellationToken cancellationToken = default)
        => _repository.GetBalanceAsync(userId, cancellationToken);

    private static string NormalizeUsername(string username)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(username);
        var normalized = username.Trim().ToLowerInvariant();
        if (normalized.Length is < 3 or > 32)
        {
            throw new InvalidOperationException("Username must be between 3 and 32 characters.");
        }

        foreach (var c in normalized)
        {
            if (!(char.IsAsciiLetterOrDigit(c) || c is '_' or '-' or '.'))
            {
                throw new InvalidOperationException("Username contains unsupported characters.");
            }
        }

        return normalized;
    }

    private static void ValidatePassword(string password)
    {
        if (string.IsNullOrWhiteSpace(password) || password.Trim().Length < 10)
        {
            throw new InvalidOperationException("Password must be at least 10 characters.");
        }
    }

    private static long GetConfiguredSignupCreditAtomic(PoolSettings settings)
    {
        if (!AmountUtility.TryParseToAtomic(settings.NewAccountCreditAmount, out var atomic))
        {
            throw new InvalidOperationException("Configured new account credit amount is invalid.");
        }

        return atomic;
    }

    private async Task<long?> TryGetPoolOnChainBalanceAsync(PoolSettings settings, CancellationToken cancellationToken)
    {
        if (GetConfiguredSignupCreditAtomic(settings) <= 0)
        {
            return null;
        }

        if (!HexUtility.IsHex(settings.PoolMinerPublicKey, 32))
        {
            _logger.Warn("Auth", "Skipping new account credit because the pool address is not configured.");
            return null;
        }

        try
        {
            var addressState = await _nodeClient.GetAddressAsync(settings.PoolMinerPublicKey, cancellationToken).ConfigureAwait(false);
            if (addressState is null)
            {
                _logger.Warn("Auth", "Skipping new account credit because the pool address state is unavailable.");
                return null;
            }

            if (!ulong.TryParse(addressState.BalanceAtomic, NumberStyles.None, CultureInfo.InvariantCulture, out var balanceAtomic))
            {
                _logger.Warn("Auth", "Skipping new account credit because the node returned an invalid pool balance.");
                return null;
            }

            if (balanceAtomic > long.MaxValue)
            {
                _logger.Warn("Auth", "Skipping new account credit because the pool balance exceeds the supported range.");
                return null;
            }

            return (long)balanceAtomic;
        }
        catch (Exception ex)
        {
            _logger.Warn("Auth", $"Skipping new account credit because the pool balance could not be loaded: {ex.Message}");
            return null;
        }
    }
}

public sealed class MinerAuthService
{
    private readonly PoolRepository _repository;

    public MinerAuthService(PoolRepository repository)
    {
        _repository = repository;
    }

    public async Task<MinerBindingResult> BindCustodianWalletAsync(PoolUser user, PoolSettings settings, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(user.CustodianPublicKeyHex))
        {
            throw new InvalidOperationException("Create a custodian wallet keypair first.");
        }

        var now = DateTimeOffset.UtcNow;
        var existing = await _repository.GetMinerByUserIdAsync(user.UserId, cancellationToken).ConfigureAwait(false);
        var publicKeyHex = HexUtility.NormalizeLower(user.CustodianPublicKeyHex, 32);
        var rawToken = existing is not null &&
                       string.Equals(existing.PublicKeyHex, publicKeyHex, StringComparison.Ordinal) &&
                       !string.IsNullOrWhiteSpace(existing.ApiTokenText)
            ? existing.ApiTokenText!
            : HexUtility.CreateTokenHex(32);

        var miner = new MinerRecord(
            existing?.MinerId ?? Guid.NewGuid().ToString("N"),
            user.UserId,
            publicKeyHex,
            existing?.ShareDifficulty ?? settings.DefaultShareDifficulty,
            HexUtility.HashSha256Hex(rawToken),
            rawToken,
            true,
            now,
            now,
            existing?.LastShareUtc);

        await _repository.UpsertMinerAsync(miner, cancellationToken).ConfigureAwait(false);
        await _repository.UpdateUserWithdrawalAddressAsync(user.UserId, publicKeyHex, cancellationToken).ConfigureAwait(false);
        return new MinerBindingResult(miner, rawToken);
    }

    public async Task<MinerRecord?> GetMinerFromApiTokenAsync(string? rawToken, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(rawToken))
        {
            return null;
        }

        return await _repository.GetMinerByApiTokenHashAsync(HexUtility.HashSha256Hex(rawToken), cancellationToken).ConfigureAwait(false);
    }
}

public sealed class LedgerService
{
    private readonly PoolRepository _repository;
    private readonly QadoNodeClient _nodeClient;
    private readonly SecretProtector _secretProtector;
    private readonly PoolSettingsStore _settingsStore;
    private readonly PoolLogger _logger;
    // All pool withdrawals spend from the same on-chain account, so nonce allocation
    // and available-balance debits need to be serialized inside this process.
    private readonly SemaphoreSlim _ledgerMutationGate = new(1, 1);

    public LedgerService(PoolRepository repository, QadoNodeClient nodeClient, SecretProtector secretProtector, PoolSettingsStore settingsStore, PoolLogger logger)
    {
        _repository = repository;
        _nodeClient = nodeClient;
        _secretProtector = secretProtector;
        _settingsStore = settingsStore;
        _logger = logger;
    }

    public async Task TransferInternalAsync(string fromUserId, string recipientUsername, long amountAtomic, string note, CancellationToken cancellationToken = default)
    {
        if (amountAtomic <= 0)
        {
            throw new InvalidOperationException("Transfer amount must be positive.");
        }

        await _ledgerMutationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            recipientUsername = recipientUsername.Trim().ToLowerInvariant();

            await _repository.ExecuteInTransactionAsync(async (connection, transaction) =>
            {
                var sender = await _repository.GetUserByIdAsync(fromUserId, connection, transaction, cancellationToken).ConfigureAwait(false)
                    ?? throw new InvalidOperationException("Sender not found.");
                var recipient = await _repository.GetUserByUsernameAsync(recipientUsername, connection, transaction, cancellationToken).ConfigureAwait(false)
                    ?? throw new InvalidOperationException("Recipient not found.");

                if (string.Equals(sender.UserId, recipient.UserId, StringComparison.Ordinal))
                {
                    throw new InvalidOperationException("Cannot transfer to the same account.");
                }

                var senderBalance = await _repository.GetBalanceAsync(sender.UserId, connection, transaction, cancellationToken).ConfigureAwait(false)
                    ?? throw new InvalidOperationException("Sender balance not found.");

                if (senderBalance.AvailableAtomic < amountAtomic)
                {
                    throw new InvalidOperationException("Insufficient available balance.");
                }

                await _repository.UpdateBalanceAsync(sender.UserId, -amountAtomic, 0, 0, 0, 0, connection, transaction, cancellationToken).ConfigureAwait(false);
                await _repository.UpdateBalanceAsync(recipient.UserId, amountAtomic, 0, 0, 0, 0, connection, transaction, cancellationToken).ConfigureAwait(false);

                var timestamp = DateTimeOffset.UtcNow;
                await _repository.InsertLedgerEntryAsync(
                    new LedgerEntryRecord(Guid.NewGuid().ToString("N"), sender.UserId, LedgerEntryType.InternalTransferOut, -amountAtomic, $"transfer:{recipient.Username}", JsonSerializer.Serialize(new { to = recipient.Username, note }), timestamp),
                    connection,
                    transaction,
                    cancellationToken).ConfigureAwait(false);

                await _repository.InsertLedgerEntryAsync(
                    new LedgerEntryRecord(Guid.NewGuid().ToString("N"), recipient.UserId, LedgerEntryType.InternalTransferIn, amountAtomic, $"transfer:{sender.Username}", JsonSerializer.Serialize(new { from = sender.Username, note }), timestamp),
                    connection,
                    transaction,
                    cancellationToken).ConfigureAwait(false);
            }, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _ledgerMutationGate.Release();
        }
    }

    public async Task<WithdrawalRequestRecord> RequestWithdrawalAsync(PoolUser user, long amountAtomic, long feeAtomic, CancellationToken cancellationToken = default)
    {
        if (amountAtomic <= 0)
        {
            throw new InvalidOperationException("Withdrawal amount must be positive.");
        }

        if (feeAtomic < 0)
        {
            throw new InvalidOperationException("Withdrawal fee must not be negative.");
        }

        if (feeAtomic > amountAtomic)
        {
            throw new InvalidOperationException("Withdrawal fee must not exceed the withdrawal amount.");
        }

        var netAmountAtomic = amountAtomic - feeAtomic;
        if (netAmountAtomic <= 0)
        {
            throw new InvalidOperationException("Withdrawal amount must stay positive after subtracting the fee.");
        }

        await _ledgerMutationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var currentBalance = await _repository.GetBalanceAsync(user.UserId, cancellationToken).ConfigureAwait(false);
            var availableAtomic = currentBalance?.AvailableAtomic ?? 0;
            if (availableAtomic < amountAtomic)
            {
                throw new InvalidOperationException(
                    $"Insufficient available balance. Requested {AmountUtility.FormatAtomic(amountAtomic)} QADO, available {AmountUtility.FormatAtomic(availableAtomic)} QADO.");
            }

            var settings = await _settingsStore.LoadAsync(cancellationToken).ConfigureAwait(false);

            var miner = await _repository.GetMinerByUserIdAsync(user.UserId, cancellationToken).ConfigureAwait(false);
            if (miner is null || !miner.IsVerified)
            {
                throw new InvalidOperationException("Verify a miner binding key before creating withdrawals.");
            }

            var resolvedAddress = HexUtility.NormalizeLower(miner.PublicKeyHex, 32);
            var poolPrivateKeyHex = _secretProtector.TryUnprotect(settings.ProtectedPoolMinerPrivateKey)
                ?? throw new InvalidOperationException("Pool private key is not configured.");
            var derivedPoolPublicKey = KeyUtility.DeriveEd25519PublicKeyHex(poolPrivateKeyHex);
            if (!string.Equals(derivedPoolPublicKey, settings.PoolMinerPublicKey, StringComparison.Ordinal))
            {
                throw new InvalidOperationException("Configured pool private key does not match the pool public key.");
            }

            var poolAddressState = await _nodeClient.GetAddressAsync(settings.PoolMinerPublicKey, cancellationToken).ConfigureAwait(false)
                ?? throw new InvalidOperationException("Pool address is not known by the node.");
            if (!ulong.TryParse(poolAddressState.BalanceAtomic, NumberStyles.None, CultureInfo.InvariantCulture, out var poolOnChainBalance))
            {
                throw new InvalidOperationException("Pool address balance returned by the node is invalid.");
            }

            var totalCostAtomic = checked((ulong)amountAtomic);
            if (poolOnChainBalance < totalCostAtomic)
            {
                throw new InvalidOperationException("Pool address does not have enough on-chain balance for this withdrawal.");
            }

            if (!ulong.TryParse(poolAddressState.Nonce, NumberStyles.None, CultureInfo.InvariantCulture, out var confirmedNonce))
            {
                throw new InvalidOperationException("Pool address nonce returned by the node is invalid.");
            }

            var nextNonce = checked(confirmedNonce + (ulong)Math.Max(0, poolAddressState.PendingOutgoingCount) + 1UL);
            var network = await _nodeClient.GetNetworkAsync(cancellationToken).ConfigureAwait(false);
            if (!uint.TryParse(network.ChainId, NumberStyles.None, CultureInfo.InvariantCulture, out var chainId) || chainId == 0)
            {
                throw new InvalidOperationException("Node returned an invalid chain id.");
            }

            var withdrawalId = Guid.NewGuid().ToString("N");
            var rawTransactionHex = QadoTransactionUtility.BuildSignedRawTransactionHex(
                chainId,
                poolPrivateKeyHex,
                resolvedAddress,
                checked((ulong)netAmountAtomic),
                checked((ulong)feeAtomic),
                nextNonce);

            var broadcast = await _nodeClient.BroadcastTransactionAsync(rawTransactionHex, withdrawalId, cancellationToken).ConfigureAwait(false);
            if (!broadcast.Accepted)
            {
                throw new InvalidOperationException($"Node rejected withdrawal transaction: {broadcast.Error ?? broadcast.Status ?? "unknown"}.");
            }

            var txId = string.IsNullOrWhiteSpace(broadcast.TxId)
                ? QadoTransactionUtility.ComputeTransactionIdHex(rawTransactionHex)
                : broadcast.TxId;

            try
            {
                return await _repository.ExecuteInTransactionAsync(async (connection, transaction) =>
                {
                    var balance = await _repository.GetBalanceAsync(user.UserId, connection, transaction, cancellationToken).ConfigureAwait(false)
                        ?? throw new InvalidOperationException("Balance not found.");

                    if (balance.AvailableAtomic < amountAtomic)
                    {
                        throw new InvalidOperationException(
                            $"Insufficient available balance. Requested {AmountUtility.FormatAtomic(amountAtomic)} QADO, available {AmountUtility.FormatAtomic(balance.AvailableAtomic)} QADO.");
                    }

                    await _repository.UpdateUserWithdrawalAddressAsync(user.UserId, resolvedAddress, connection, transaction, cancellationToken).ConfigureAwait(false);
                    await _repository.UpdateBalanceAsync(user.UserId, -amountAtomic, 0, 0, 0, amountAtomic, connection, transaction, cancellationToken).ConfigureAwait(false);

                    var request = new WithdrawalRequestRecord(
                        withdrawalId,
                        user.UserId,
                        amountAtomic,
                        resolvedAddress,
                        WithdrawalStatus.Paid,
                        $"auto-broadcast fee={AmountUtility.FormatAtomic(feeAtomic)} net={AmountUtility.FormatAtomic(netAmountAtomic)}",
                        txId,
                        DateTimeOffset.UtcNow,
                        DateTimeOffset.UtcNow);

                    await _repository.InsertWithdrawalRequestAsync(request, connection, transaction, cancellationToken).ConfigureAwait(false);
                    await _repository.InsertLedgerEntryAsync(
                        new LedgerEntryRecord(
                            Guid.NewGuid().ToString("N"),
                            user.UserId,
                            LedgerEntryType.WithdrawalReserve,
                            -amountAtomic,
                            $"withdraw:{request.WithdrawalId}",
                            JsonSerializer.Serialize(new
                            {
                                address = resolvedAddress,
                                txid = txId,
                                feeAtomic,
                                fee = AmountUtility.FormatAtomic(feeAtomic),
                                requestedAmountAtomic = amountAtomic,
                                requestedAmount = AmountUtility.FormatAtomic(amountAtomic),
                                sentAmountAtomic = netAmountAtomic,
                                sentAmount = AmountUtility.FormatAtomic(netAmountAtomic),
                                nonce = nextNonce
                            }),
                            DateTimeOffset.UtcNow),
                        connection,
                        transaction,
                        cancellationToken).ConfigureAwait(false);

                    _logger.Info("Withdrawal", $"Auto-broadcast withdrawal {withdrawalId} txid={txId} user={user.Username} gross={AmountUtility.FormatAtomic(amountAtomic)} fee={AmountUtility.FormatAtomic(feeAtomic)} net={AmountUtility.FormatAtomic(netAmountAtomic)}");
                    return request;
                }, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.Error("Withdrawal", ex, $"Broadcasted withdrawal {withdrawalId} txid={txId} for user={user.Username}, but ledger persistence failed.");
                throw new InvalidOperationException(
                    $"Withdrawal transaction {txId} was broadcast successfully, but the local ledger update failed. Manual reconciliation is required.",
                    ex);
            }
        }
        finally
        {
            _ledgerMutationGate.Release();
        }
    }

    public async Task MarkWithdrawalPaidAsync(string withdrawalId, string externalTxId, string? note, CancellationToken cancellationToken = default)
    {
        await _repository.ExecuteInTransactionAsync(async (connection, transaction) =>
        {
            var request = await _repository.GetWithdrawalByIdAsync(withdrawalId, connection, transaction, cancellationToken).ConfigureAwait(false)
                ?? throw new InvalidOperationException("Withdrawal request not found.");

            if (request.Status is not (WithdrawalStatus.Queued or WithdrawalStatus.AwaitingAdmin))
            {
                throw new InvalidOperationException("Withdrawal is not pending.");
            }

            await _repository.UpdateBalanceAsync(request.UserId, 0, -request.AmountAtomic, 0, 0, request.AmountAtomic, connection, transaction, cancellationToken).ConfigureAwait(false);
            await _repository.UpdateWithdrawalRequestAsync(request.WithdrawalId, WithdrawalStatus.Paid, note, externalTxId, connection, transaction, cancellationToken).ConfigureAwait(false);
            await _repository.InsertLedgerEntryAsync(
                new LedgerEntryRecord(Guid.NewGuid().ToString("N"), request.UserId, LedgerEntryType.WithdrawalRelease, 0, $"withdraw-paid:{request.WithdrawalId}", JsonSerializer.Serialize(new { txid = externalTxId, note }), DateTimeOffset.UtcNow),
                connection,
                transaction,
                cancellationToken).ConfigureAwait(false);
        }, cancellationToken).ConfigureAwait(false);
    }

    public async Task RejectWithdrawalAsync(string withdrawalId, string? note, CancellationToken cancellationToken = default)
    {
        await _repository.ExecuteInTransactionAsync(async (connection, transaction) =>
        {
            var request = await _repository.GetWithdrawalByIdAsync(withdrawalId, connection, transaction, cancellationToken).ConfigureAwait(false)
                ?? throw new InvalidOperationException("Withdrawal request not found.");

            if (request.Status is not (WithdrawalStatus.Queued or WithdrawalStatus.AwaitingAdmin))
            {
                throw new InvalidOperationException("Withdrawal is not pending.");
            }

            await _repository.UpdateBalanceAsync(request.UserId, request.AmountAtomic, -request.AmountAtomic, 0, 0, 0, connection, transaction, cancellationToken).ConfigureAwait(false);
            await _repository.UpdateWithdrawalRequestAsync(request.WithdrawalId, WithdrawalStatus.Rejected, note, null, connection, transaction, cancellationToken).ConfigureAwait(false);
            await _repository.InsertLedgerEntryAsync(
                new LedgerEntryRecord(Guid.NewGuid().ToString("N"), request.UserId, LedgerEntryType.WithdrawalRelease, request.AmountAtomic, $"withdraw-reject:{request.WithdrawalId}", JsonSerializer.Serialize(new { note }), DateTimeOffset.UtcNow),
                connection,
                transaction,
                cancellationToken).ConfigureAwait(false);
        }, cancellationToken).ConfigureAwait(false);
    }
}
