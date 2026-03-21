using System.IO;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace QadoPoolStack.Desktop.Configuration;

public sealed class PoolSettings
{
    public string NodeBaseUrl { get; set; } = "http://127.0.0.1:18080";

    public int HttpPort { get; set; } = 80;

    public int HttpsPort { get; set; } = 443;

    public bool EnableHttps { get; set; } = false;

    public bool PreferHttpsWhenAvailable { get; set; } = false;

    public string DomainName { get; set; } = "";

    public string LetsEncryptEmail { get; set; } = "";

    public bool UseLetsEncryptStaging { get; set; } = true;

    public string CertificatePath { get; set; } = "";

    public string ProtectedCertificatePassword { get; set; } = "";

    public string PoolMinerPublicKey { get; set; } = "";

    public string ProtectedPoolMinerPrivateKey { get; set; } = "";

    public double DefaultShareDifficulty { get; set; } = 75_000_000d;

    public int ShareTargetSecondsMin { get; set; } = 5;

    public int ShareTargetSecondsMax { get; set; } = 10;

    public int ShareJobLifetimeSeconds { get; set; } = 30;

    public int ChallengeLifetimeSeconds { get; set; } = 300;

    public int SessionLifetimeHours { get; set; } = 168;

    public int AuthRateLimitPerMinute { get; set; } = 20;

    public int UserApiRateLimitPerMinute { get; set; } = 120;

    public int MinerRequestRateLimitPerMinute { get; set; } = 120;

    public int ShareRateLimitPerMinute { get; set; } = 240;

    public int PoolFeeBasisPoints { get; set; } = 100;

    public bool AccountRegistrationEnabled { get; set; } = true;

    public int AddressPollSeconds { get; set; } = 20;

    public int DepositMinConfirmations { get; set; } = 6;

    public PoolSettings Clone()
    {
        return new PoolSettings
        {
            NodeBaseUrl = NodeBaseUrl,
            HttpPort = HttpPort,
            HttpsPort = HttpsPort,
            EnableHttps = EnableHttps,
            PreferHttpsWhenAvailable = PreferHttpsWhenAvailable,
            DomainName = DomainName,
            LetsEncryptEmail = LetsEncryptEmail,
            UseLetsEncryptStaging = UseLetsEncryptStaging,
            CertificatePath = CertificatePath,
            ProtectedCertificatePassword = ProtectedCertificatePassword,
            PoolMinerPublicKey = PoolMinerPublicKey,
            ProtectedPoolMinerPrivateKey = ProtectedPoolMinerPrivateKey,
            DefaultShareDifficulty = DefaultShareDifficulty,
            ShareTargetSecondsMin = ShareTargetSecondsMin,
            ShareTargetSecondsMax = ShareTargetSecondsMax,
            ShareJobLifetimeSeconds = ShareJobLifetimeSeconds,
            ChallengeLifetimeSeconds = ChallengeLifetimeSeconds,
            SessionLifetimeHours = SessionLifetimeHours,
            AuthRateLimitPerMinute = AuthRateLimitPerMinute,
            UserApiRateLimitPerMinute = UserApiRateLimitPerMinute,
            MinerRequestRateLimitPerMinute = MinerRequestRateLimitPerMinute,
            ShareRateLimitPerMinute = ShareRateLimitPerMinute,
            PoolFeeBasisPoints = PoolFeeBasisPoints,
            AccountRegistrationEnabled = AccountRegistrationEnabled,
            AddressPollSeconds = AddressPollSeconds,
            DepositMinConfirmations = DepositMinConfirmations
        };
    }

    public bool SemanticallyEquals(PoolSettings? other)
    {
        if (other is null)
        {
            return false;
        }

        return string.Equals(NodeBaseUrl, other.NodeBaseUrl, StringComparison.Ordinal) &&
               HttpPort == other.HttpPort &&
               HttpsPort == other.HttpsPort &&
               EnableHttps == other.EnableHttps &&
               PreferHttpsWhenAvailable == other.PreferHttpsWhenAvailable &&
               string.Equals(DomainName, other.DomainName, StringComparison.Ordinal) &&
               string.Equals(LetsEncryptEmail, other.LetsEncryptEmail, StringComparison.Ordinal) &&
               UseLetsEncryptStaging == other.UseLetsEncryptStaging &&
               string.Equals(CertificatePath, other.CertificatePath, StringComparison.Ordinal) &&
               string.Equals(ProtectedCertificatePassword, other.ProtectedCertificatePassword, StringComparison.Ordinal) &&
               string.Equals(PoolMinerPublicKey, other.PoolMinerPublicKey, StringComparison.Ordinal) &&
               string.Equals(ProtectedPoolMinerPrivateKey, other.ProtectedPoolMinerPrivateKey, StringComparison.Ordinal) &&
               DefaultShareDifficulty.Equals(other.DefaultShareDifficulty) &&
               ShareTargetSecondsMin == other.ShareTargetSecondsMin &&
               ShareTargetSecondsMax == other.ShareTargetSecondsMax &&
               ShareJobLifetimeSeconds == other.ShareJobLifetimeSeconds &&
               ChallengeLifetimeSeconds == other.ChallengeLifetimeSeconds &&
               SessionLifetimeHours == other.SessionLifetimeHours &&
               AuthRateLimitPerMinute == other.AuthRateLimitPerMinute &&
               UserApiRateLimitPerMinute == other.UserApiRateLimitPerMinute &&
               MinerRequestRateLimitPerMinute == other.MinerRequestRateLimitPerMinute &&
               ShareRateLimitPerMinute == other.ShareRateLimitPerMinute &&
               PoolFeeBasisPoints == other.PoolFeeBasisPoints &&
               AccountRegistrationEnabled == other.AccountRegistrationEnabled &&
               AddressPollSeconds == other.AddressPollSeconds &&
               DepositMinConfirmations == other.DepositMinConfirmations;
    }

    public static PoolSettings CreateDefault() => new();
}

public sealed class PoolSettingsStore
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    private readonly AppPaths _paths;

    public PoolSettingsStore(AppPaths paths)
    {
        _paths = paths;
    }

    public async Task<PoolSettings> LoadAsync(CancellationToken cancellationToken = default)
    {
        _paths.EnsureDirectories();

        if (!File.Exists(_paths.SettingsFilePath))
        {
            var defaults = PoolSettings.CreateDefault();
            await SaveAsync(defaults, cancellationToken).ConfigureAwait(false);
            return defaults;
        }

        await using var stream = File.OpenRead(_paths.SettingsFilePath);
        var settings = await JsonSerializer.DeserializeAsync<PoolSettings>(stream, JsonOptions, cancellationToken).ConfigureAwait(false);
        return settings ?? PoolSettings.CreateDefault();
    }

    public async Task SaveAsync(PoolSettings settings, CancellationToken cancellationToken = default)
    {
        _paths.EnsureDirectories();

        await using var stream = File.Create(_paths.SettingsFilePath);
        await JsonSerializer.SerializeAsync(stream, settings, JsonOptions, cancellationToken).ConfigureAwait(false);
    }
}
