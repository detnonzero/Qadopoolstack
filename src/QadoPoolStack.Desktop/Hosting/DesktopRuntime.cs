using QadoPoolStack.Desktop.Configuration;
using QadoPoolStack.Desktop.Domain;
using QadoPoolStack.Desktop.Infrastructure.Logging;
using QadoPoolStack.Desktop.Infrastructure.Security;
using QadoPoolStack.Desktop.Persistence;
using QadoPoolStack.Desktop.Services.Accounts;
using QadoPoolStack.Desktop.Services.Tls;
using QadoPoolStack.Desktop.Utilities;

namespace QadoPoolStack.Desktop.Hosting;

public sealed class DesktopRuntime
{
    private int _shutdownStarted;

    private readonly PoolSettingsStore _settingsStore;
    private readonly PoolRepository _repository;
    private readonly PoolServerHost _serverHost;
    private readonly LetsEncryptService _letsEncryptService;
    private readonly LedgerService _ledgerService;
    private readonly PoolLogger _logger;
    private readonly SecretProtector _secretProtector;

    public DesktopRuntime(
        AppPaths paths,
        PoolSettingsStore settingsStore,
        PoolRepository repository,
        PoolServerHost serverHost,
        LetsEncryptService letsEncryptService,
        LedgerService ledgerService,
        PoolLogger logger,
        SecretProtector secretProtector)
    {
        Paths = paths;
        _settingsStore = settingsStore;
        _repository = repository;
        _serverHost = serverHost;
        _letsEncryptService = letsEncryptService;
        _ledgerService = ledgerService;
        _logger = logger;
        _secretProtector = secretProtector;
    }

    public AppPaths Paths { get; }

    public PoolSettings Settings { get; private set; } = PoolSettings.CreateDefault();

    public PoolLogger Logger => _logger;

    public bool ServerRunning => _serverHost.IsRunning;

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        Paths.EnsureDirectories();
        Settings = (await _settingsStore.LoadAsync(cancellationToken).ConfigureAwait(false)).Clone();
        await _repository.InitializeAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task SaveSettingsAsync(PoolSettings settings, CancellationToken cancellationToken = default)
    {
        Settings = settings.Clone();
        await _settingsStore.SaveAsync(Settings, cancellationToken).ConfigureAwait(false);
    }

    public async Task SetAccountRegistrationEnabledAsync(bool enabled, CancellationToken cancellationToken = default)
    {
        Settings.AccountRegistrationEnabled = enabled;
        await _settingsStore.SaveAsync(Settings, cancellationToken).ConfigureAwait(false);
        _serverHost.UpdateAccountRegistrationEnabled(enabled);
    }

    public async Task StartServerAsync(CancellationToken cancellationToken = default)
    {
        await _serverHost.StartAsync(Settings.Clone(), cancellationToken).ConfigureAwait(false);
    }

    public Task StopServerAsync(CancellationToken cancellationToken = default)
        => _serverHost.StopAsync(cancellationToken);

    public async Task ShutdownAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref _shutdownStarted, 1) != 0)
        {
            return;
        }

        await StopServerAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<string> GeneratePoolMinerKeyAsync(CancellationToken cancellationToken = default)
    {
        var (privateKey, publicKey) = KeyUtility.GenerateEd25519KeypairHex();
        await SetPoolMinerPrivateKeyAsync(privateKey, cancellationToken).ConfigureAwait(false);
        return publicKey;
    }

    public string? GetPoolMinerPrivateKey()
        => _secretProtector.TryUnprotect(Settings.ProtectedPoolMinerPrivateKey);

    public async Task<string> SetPoolMinerPrivateKeyAsync(string privateKeyHex, CancellationToken cancellationToken = default)
    {
        privateKeyHex = HexUtility.NormalizeLower(privateKeyHex, 32);
        var publicKey = KeyUtility.DeriveEd25519PublicKeyHex(privateKeyHex);
        Settings.PoolMinerPublicKey = publicKey;
        Settings.ProtectedPoolMinerPrivateKey = _secretProtector.Protect(privateKeyHex);
        await _settingsStore.SaveAsync(Settings, cancellationToken).ConfigureAwait(false);
        return publicKey;
    }

    public async Task ClearPoolMinerKeyAsync(CancellationToken cancellationToken = default)
    {
        Settings.PoolMinerPublicKey = string.Empty;
        Settings.ProtectedPoolMinerPrivateKey = string.Empty;
        await _settingsStore.SaveAsync(Settings, cancellationToken).ConfigureAwait(false);
    }

    public async Task AcquireTlsCertificateAsync(CancellationToken cancellationToken = default)
    {
        var result = await _letsEncryptService.AcquireAsync(Settings.LetsEncryptEmail, Settings.DomainName, Settings.UseLetsEncryptStaging, cancellationToken).ConfigureAwait(false);
        Settings.CertificatePath = result.CertificatePath;
        Settings.ProtectedCertificatePassword = result.ProtectedPassword;
        Settings.EnableHttps = true;
        await _settingsStore.SaveAsync(Settings, cancellationToken).ConfigureAwait(false);
    }

    public async Task<DashboardSnapshot> GetDashboardAsync(CancellationToken cancellationToken = default)
    {
        var openRound = await _repository.GetOpenRoundAsync(cancellationToken).ConfigureAwait(false);
        var users = await _repository.CountUsersAsync(cancellationToken).ConfigureAwait(false);
        var miners = await _repository.CountVerifiedMinersAsync(cancellationToken).ConfigureAwait(false);
        var withdrawals = await _repository.CountPendingWithdrawalsAsync(cancellationToken).ConfigureAwait(false);
        var shares = await _repository.CountOpenRoundSharesAsync(cancellationToken).ConfigureAwait(false);
        var tracked = await _repository.GetTotalTrackedBalanceAsync(cancellationToken).ConfigureAwait(false);

        return new DashboardSnapshot(
            ServerRunning,
            openRound?.HeightText ?? "0",
            openRound?.PrevHashHex ?? "",
            users,
            miners,
            withdrawals,
            shares,
            tracked.ToString());
    }

    public bool HasPendingServerSettingsChanges()
        => _serverHost.HasPendingSettingsChanges(Settings);

    public string GetServerAccessSummary()
        => _serverHost.GetRunningAccessSummary() ?? PoolAccessPreference.BuildSummary(Settings);

    public string? GetPendingServerAccessSummary()
        => HasPendingServerSettingsChanges() ? PoolAccessPreference.BuildSummary(Settings) : null;

    public Task<List<UserBalanceView>> ListUsersAsync(CancellationToken cancellationToken = default)
        => _repository.ListUsersWithBalancesAsync(cancellationToken);

    public async Task<WithdrawalRequestRecord> WithdrawAllForUserAsync(string userId, CancellationToken cancellationToken = default)
    {
        var user = await _repository.GetUserByIdAsync(userId, cancellationToken).ConfigureAwait(false)
            ?? throw new InvalidOperationException("User not found.");
        var balance = await _repository.GetBalanceAsync(userId, cancellationToken).ConfigureAwait(false);
        var availableAtomic = balance?.AvailableAtomic ?? 0;
        if (availableAtomic <= 0)
        {
            throw new InvalidOperationException("This user has no available balance to withdraw.");
        }

        return await _ledgerService.RequestWithdrawalAsync(user, availableAtomic, 0, cancellationToken).ConfigureAwait(false);
    }

    public Task<List<WithdrawalRequestRecord>> ListWithdrawalsAsync(CancellationToken cancellationToken = default)
        => _repository.ListWithdrawalRequestsAsync(null, cancellationToken);

    public Task<List<PoolRound>> ListRecentRoundsAsync(CancellationToken cancellationToken = default)
        => _repository.ListRecentRoundsAsync(15, cancellationToken);

    public Task<List<FoundBlockRecord>> ListRecentBlocksAsync(CancellationToken cancellationToken = default)
        => _repository.ListRecentBlocksAsync(15, cancellationToken);
}
