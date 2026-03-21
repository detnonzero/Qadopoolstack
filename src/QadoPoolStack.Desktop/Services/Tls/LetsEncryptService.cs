using System.Collections.Concurrent;
using System.IO;
using Certes;
using Certes.Acme;
using Certes.Acme.Resource;
using QadoPoolStack.Desktop.Configuration;
using QadoPoolStack.Desktop.Infrastructure.Logging;
using QadoPoolStack.Desktop.Infrastructure.Security;
using QadoPoolStack.Desktop.Utilities;

namespace QadoPoolStack.Desktop.Services.Tls;

public sealed class AcmeChallengeStore
{
    private readonly ConcurrentDictionary<string, string> _tokens = new(StringComparer.Ordinal);

    public void Set(string token, string content) => _tokens[token] = content;

    public bool TryGet(string token, out string? content) => _tokens.TryGetValue(token, out content);

    public void Remove(string token) => _tokens.TryRemove(token, out _);
}

public sealed record CertificateAcquisitionResult(string CertificatePath, string ProtectedPassword);

public sealed class LetsEncryptService
{
    private readonly AppPaths _paths;
    private readonly SecretProtector _secretProtector;
    private readonly AcmeChallengeStore _challengeStore;
    private readonly PoolLogger _logger;

    public LetsEncryptService(AppPaths paths, SecretProtector secretProtector, AcmeChallengeStore challengeStore, PoolLogger logger)
    {
        _paths = paths;
        _secretProtector = secretProtector;
        _challengeStore = challengeStore;
        _logger = logger;
    }

    public async Task<CertificateAcquisitionResult> AcquireAsync(string email, string domainName, bool useStaging, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(email))
        {
            throw new InvalidOperationException("LetsEncrypt email is required.");
        }

        if (string.IsNullOrWhiteSpace(domainName))
        {
            throw new InvalidOperationException("Domain name is required.");
        }

        _paths.EnsureDirectories();

        var server = useStaging ? WellKnownServers.LetsEncryptStagingV2 : WellKnownServers.LetsEncryptV2;
        var acme = new AcmeContext(server);
        await acme.NewAccount(email, true).ConfigureAwait(false);

        var order = await acme.NewOrder([domainName]).ConfigureAwait(false);
        var authorizations = await order.Authorizations().ConfigureAwait(false);

        foreach (var authorizationContext in authorizations)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var challenge = await authorizationContext.Http().ConfigureAwait(false);
            _challengeStore.Set(challenge.Token, challenge.KeyAuthz);

            try
            {
                await challenge.Validate().ConfigureAwait(false);
                await WaitForAuthorizationAsync(authorizationContext, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                _challengeStore.Remove(challenge.Token);
            }
        }

        var certificateKey = KeyFactory.NewKey(KeyAlgorithm.ES256);
        var certificateChain = await order.Generate(new CsrInfo
        {
            CommonName = domainName
        }, certificateKey).ConfigureAwait(false);

        var password = HexUtility.CreateTokenHex(16);
        var pfxBytes = certificateChain.ToPfx(certificateKey).Build(domainName, password);
        var certificatePath = Path.Combine(_paths.CertificatesDirectory, $"{domainName.Replace('*', '_')}.pfx");
        await File.WriteAllBytesAsync(certificatePath, pfxBytes, cancellationToken).ConfigureAwait(false);

        _logger.Info("TLS", $"Stored certificate for {domainName} at {certificatePath}");
        return new CertificateAcquisitionResult(certificatePath, _secretProtector.Protect(password));
    }

    private static async Task WaitForAuthorizationAsync(IAuthorizationContext authorizationContext, CancellationToken cancellationToken)
    {
        for (var attempt = 0; attempt < 20; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var resource = await authorizationContext.Resource().ConfigureAwait(false);
            if (resource.Status == AuthorizationStatus.Valid)
            {
                return;
            }

            if (resource.Status == AuthorizationStatus.Invalid)
            {
                throw new InvalidOperationException("LetsEncrypt challenge validation failed.");
            }

            await Task.Delay(TimeSpan.FromSeconds(3), cancellationToken).ConfigureAwait(false);
        }

        throw new TimeoutException("LetsEncrypt authorization did not validate in time.");
    }
}
