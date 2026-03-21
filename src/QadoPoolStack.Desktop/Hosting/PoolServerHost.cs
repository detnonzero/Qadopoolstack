using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.RateLimiting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.FileProviders;
using Microsoft.Extensions.Hosting;
using QadoPoolStack.Desktop.Api;
using QadoPoolStack.Desktop.Configuration;
using QadoPoolStack.Desktop.Infrastructure.Logging;
using QadoPoolStack.Desktop.Infrastructure.Security;
using QadoPoolStack.Desktop.Persistence;
using QadoPoolStack.Desktop.Services.Accounts;
using QadoPoolStack.Desktop.Services.Background;
using QadoPoolStack.Desktop.Services.Mining;
using QadoPoolStack.Desktop.Services.Node;
using QadoPoolStack.Desktop.Services.Tls;
using System.Globalization;
using System.IO;
using System.Threading.RateLimiting;

namespace QadoPoolStack.Desktop.Hosting;

public sealed class PoolServerHost
{
    private readonly AppPaths _paths;
    private readonly PoolRepository _repository;
    private readonly SecretProtector _secretProtector;
    private readonly PoolLogger _logger;
    private readonly AcmeChallengeStore _acmeChallengeStore;
    private WebApplication? _app;
    private PoolSettings? _runningSettingsSnapshot;

    public PoolServerHost(AppPaths paths, PoolRepository repository, SecretProtector secretProtector, PoolLogger logger, AcmeChallengeStore acmeChallengeStore)
    {
        _paths = paths;
        _repository = repository;
        _secretProtector = secretProtector;
        _logger = logger;
        _acmeChallengeStore = acmeChallengeStore;
    }

    public bool IsRunning => _app is not null;

    public bool HasPendingSettingsChanges(PoolSettings settings)
        => _app is not null &&
           _runningSettingsSnapshot is not null &&
           !_runningSettingsSnapshot.SemanticallyEquals(settings);

    public string? GetRunningAccessSummary()
        => _runningSettingsSnapshot is null ? null : PoolAccessPreference.BuildSummary(_runningSettingsSnapshot);

    public void UpdateAccountRegistrationEnabled(bool enabled)
    {
        if (_runningSettingsSnapshot is null)
        {
            return;
        }

        _runningSettingsSnapshot.AccountRegistrationEnabled = enabled;
        _logger.Info("Server", $"Account registration {(enabled ? "enabled" : "disabled")}.");
    }

    public async Task StartAsync(PoolSettings settings, CancellationToken cancellationToken = default)
    {
        if (_app is not null)
        {
            return;
        }

        var settingsSnapshot = settings.Clone();

        _paths.EnsureDirectories();
        await _repository.InitializeAsync(cancellationToken).ConfigureAwait(false);

        var builder = WebApplication.CreateBuilder(new WebApplicationOptions
        {
            ContentRootPath = _paths.BaseDirectory,
            WebRootPath = _paths.WebRootPath
        });

        builder.WebHost.ConfigureKestrel(options =>
        {
            options.ListenAnyIP(settingsSnapshot.HttpPort);
            if (settingsSnapshot.EnableHttps && File.Exists(settingsSnapshot.CertificatePath))
            {
                var password = _secretProtector.TryUnprotect(settingsSnapshot.ProtectedCertificatePassword);
                options.ListenAnyIP(settingsSnapshot.HttpsPort, listen => listen.UseHttps(settingsSnapshot.CertificatePath, password));
            }
        });

        builder.Services.AddSingleton(settingsSnapshot);
        builder.Services.AddSingleton(_paths);
        builder.Services.AddSingleton<PoolSettingsStore>();
        builder.Services.AddSingleton(_repository);
        builder.Services.AddSingleton(_logger);
        builder.Services.AddSingleton(_secretProtector);
        builder.Services.AddSingleton(_acmeChallengeStore);
        builder.Services.AddSingleton<PasswordHasher>();
        builder.Services.AddSingleton<SessionService>();
        builder.Services.AddSingleton<UserAccountService>();
        builder.Services.AddSingleton<MinerAuthService>();
        builder.Services.AddSingleton<LedgerService>();
        builder.Services.AddSingleton<DepositMonitorService>();
        builder.Services.AddSingleton<MiningJobService>();
        builder.Services.AddSingleton<RoundAccountingService>();
        builder.Services.AddSingleton<ShareValidationService>();
        builder.Services.AddSingleton<DifficultyService>();
        builder.Services.AddSingleton<RoundMonitorService>();
        builder.Services.AddSingleton<LetsEncryptService>();
        builder.Services.AddHttpClient<QadoNodeClient>(client =>
        {
            client.BaseAddress = new Uri(settingsSnapshot.NodeBaseUrl.TrimEnd('/'));
            client.Timeout = TimeSpan.FromSeconds(20);
        });
        builder.Services.AddRateLimiter(options =>
        {
            options.RejectionStatusCode = StatusCodes.Status429TooManyRequests;
            options.OnRejected = async (context, cancellationToken) =>
            {
                if (context.Lease.TryGetMetadata(MetadataName.RetryAfter, out var retryAfter))
                {
                    context.HttpContext.Response.Headers.RetryAfter = Math.Ceiling(retryAfter.TotalSeconds).ToString(CultureInfo.InvariantCulture);
                }

                context.HttpContext.Response.ContentType = "application/json; charset=utf-8";
                await context.HttpContext.Response.WriteAsync("{\"error\":\"Rate limit exceeded.\"}", cancellationToken).ConfigureAwait(false);
            };

            options.AddPolicy(PoolRateLimiting.PublicAuthPolicy, httpContext =>
                RateLimitPartition.GetFixedWindowLimiter(
                    PoolRateLimiting.GetIpPartitionKey(httpContext),
                    _ => PoolRateLimiting.CreateFixedWindowOptions(settingsSnapshot.AuthRateLimitPerMinute)));

            options.AddPolicy(PoolRateLimiting.UserApiPolicy, httpContext =>
                RateLimitPartition.GetFixedWindowLimiter(
                    PoolRateLimiting.GetBearerOrIpPartitionKey(httpContext, "user"),
                    _ => PoolRateLimiting.CreateFixedWindowOptions(settingsSnapshot.UserApiRateLimitPerMinute)));

            options.AddPolicy(PoolRateLimiting.MinerApiPolicy, httpContext =>
                RateLimitPartition.GetFixedWindowLimiter(
                    PoolRateLimiting.GetMinerTokenOrIpPartitionKey(httpContext, "miner"),
                    _ => PoolRateLimiting.CreateFixedWindowOptions(settingsSnapshot.MinerRequestRateLimitPerMinute)));

            options.AddPolicy(PoolRateLimiting.ShareSubmitPolicy, httpContext =>
                RateLimitPartition.GetTokenBucketLimiter(
                    PoolRateLimiting.GetMinerTokenOrIpPartitionKey(httpContext, "share"),
                    _ => PoolRateLimiting.CreateShareSubmitOptions(settingsSnapshot.ShareRateLimitPerMinute)));
        });
        builder.Services.AddHostedService<DifficultyAdjustmentWorker>();
        builder.Services.AddHostedService<RoundFinalizationWorker>();
        builder.Services.AddHostedService<DepositPollingWorker>();
        builder.Services.AddHostedService<PayoutWorker>();

        var app = builder.Build();
        app.UseRateLimiter();
        app.UseDefaultFiles();
        app.UseStaticFiles(new StaticFileOptions
        {
            FileProvider = new PhysicalFileProvider(_paths.WebRootPath),
            ServeUnknownFileTypes = false,
            OnPrepareResponse = context => ApplyNoStoreHeaders(context.Context.Response)
        });

        app.MapGet("/.well-known/acme-challenge/{token}", (string token, AcmeChallengeStore store) =>
        {
            return store.TryGet(token, out var content)
                ? Results.Text(content!, "text/plain")
                : Results.NotFound();
        });

        app.MapGet("/register", async context =>
        {
            PrepareHtmlResponse(context.Response);
            await context.Response.SendFileAsync(Path.Combine(_paths.WebRootPath, "register.html")).ConfigureAwait(false);
        });

        app.MapGet("/dashboard", async context =>
        {
            PrepareHtmlResponse(context.Response);
            await context.Response.SendFileAsync(Path.Combine(_paths.WebRootPath, "dashboard.html")).ConfigureAwait(false);
        });

        PoolApi.Map(app);
        app.MapFallback(async context =>
        {
            PrepareHtmlResponse(context.Response);
            await context.Response.SendFileAsync(Path.Combine(_paths.WebRootPath, "index.html")).ConfigureAwait(false);
        });

        await app.StartAsync(cancellationToken).ConfigureAwait(false);
        _app = app;
        _runningSettingsSnapshot = settingsSnapshot;
        var listenerSummary = $"Pool server listening on http://0.0.0.0:{settingsSnapshot.HttpPort}" +
                              (PoolAccessPreference.IsHttpsAvailable(settingsSnapshot) ? $" and https://0.0.0.0:{settingsSnapshot.HttpsPort}" : "");
        _logger.Info("Server", $"{listenerSummary}. {PoolAccessPreference.BuildSummary(settingsSnapshot)}");
    }

    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        if (_app is null)
        {
            return;
        }

        await _app.StopAsync(cancellationToken).ConfigureAwait(false);
        await _app.DisposeAsync().ConfigureAwait(false);
        _app = null;
        _runningSettingsSnapshot = null;
        _logger.Info("Server", "Pool server stopped.");
    }

    private static void PrepareHtmlResponse(HttpResponse response)
    {
        response.ContentType = "text/html; charset=utf-8";
        ApplyNoStoreHeaders(response);
    }

    private static void ApplyNoStoreHeaders(HttpResponse response)
    {
        response.Headers.CacheControl = "no-store, no-cache, max-age=0";
        response.Headers.Pragma = "no-cache";
        response.Headers.Expires = "0";
    }
}
