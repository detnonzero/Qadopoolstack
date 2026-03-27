using Microsoft.Extensions.Hosting;
using QadoPoolStack.Desktop.Configuration;
using QadoPoolStack.Desktop.Domain;
using QadoPoolStack.Desktop.Infrastructure.Logging;
using QadoPoolStack.Desktop.Persistence;
using QadoPoolStack.Desktop.Services.Accounts;
using QadoPoolStack.Desktop.Services.Mining;

namespace QadoPoolStack.Desktop.Services.Background;

public sealed class DifficultyAdjustmentWorker : BackgroundService
{
    private readonly DifficultyService _difficultyService;
    private readonly PoolSettings _settings;
    private readonly PoolLogger _logger;

    public DifficultyAdjustmentWorker(DifficultyService difficultyService, PoolSettings settings, PoolLogger logger)
    {
        _difficultyService = difficultyService;
        _settings = settings;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(15));
        while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
        {
            try
            {
                await _difficultyService.RebalanceAsync(_settings, stoppingToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.Error("Difficulty", ex, "Difficulty rebalance failed.");
            }
        }
    }
}

public sealed class RoundFinalizationWorker : BackgroundService
{
    private readonly RoundMonitorService _roundMonitorService;
    private readonly FoundBlockSettlementService _foundBlockSettlementService;
    private readonly PoolLogger _logger;

    public RoundFinalizationWorker(RoundMonitorService roundMonitorService, FoundBlockSettlementService foundBlockSettlementService, PoolLogger logger)
    {
        _roundMonitorService = roundMonitorService;
        _foundBlockSettlementService = foundBlockSettlementService;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(10));
        while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
        {
            try
            {
                await _roundMonitorService.RefreshOpenRoundAsync(stoppingToken).ConfigureAwait(false);
                await _foundBlockSettlementService.ReconcileAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.Error("Rounds", ex, "Round monitor failed.");
            }
        }
    }
}

public sealed class DepositPollingWorker : BackgroundService
{
    private readonly DepositMonitorService _depositMonitorService;
    private readonly PoolSettings _settings;
    private readonly PoolLogger _logger;

    public DepositPollingWorker(DepositMonitorService depositMonitorService, PoolSettings settings, PoolLogger logger)
    {
        _depositMonitorService = depositMonitorService;
        _settings = settings;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(Math.Max(10, _settings.AddressPollSeconds)));
        while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
        {
            try
            {
                await _depositMonitorService.PollAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.Error("Deposit", ex, "Deposit poller failed.");
            }
        }
    }
}

public sealed class PayoutWorker : BackgroundService
{
    private readonly PoolRepository _repository;
    private readonly PoolLogger _logger;

    public PayoutWorker(PoolRepository repository, PoolLogger logger)
    {
        _repository = repository;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(20));
        while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
        {
            try
            {
                var queued = await _repository.ListWithdrawalRequestsAsync(WithdrawalStatus.Queued, stoppingToken).ConfigureAwait(false);
                foreach (var request in queued)
                {
                    await _repository.UpdateWithdrawalRequestAsync(request.WithdrawalId, WithdrawalStatus.AwaitingAdmin, "Awaiting manual payout", request.ExternalTxId, stoppingToken).ConfigureAwait(false);
                }

                if (queued.Count > 0)
                {
                    _logger.Info("Payout", $"Moved {queued.Count} withdrawal requests into manual payout review.");
                }
            }
            catch (Exception ex)
            {
                _logger.Error("Payout", ex, "Payout worker failed.");
            }
        }
    }
}
