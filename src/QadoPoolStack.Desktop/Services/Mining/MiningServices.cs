using System.Buffers.Binary;
using System.Globalization;
using System.Numerics;
using System.Text.Json;
using Blake3;
using QadoPoolStack.Desktop.Configuration;
using QadoPoolStack.Desktop.Domain;
using QadoPoolStack.Desktop.Infrastructure.Logging;
using QadoPoolStack.Desktop.Persistence;
using QadoPoolStack.Desktop.Services.Node;
using QadoPoolStack.Desktop.Utilities;

namespace QadoPoolStack.Desktop.Services.Mining;

public sealed record PoolMiningJobEnvelope(PoolJob PoolJob, QadoMiningJobResponse NodeJob);

public sealed record ShareSubmissionOutcome(
    bool Accepted,
    bool Duplicate,
    bool Stale,
    bool BlockCandidate,
    bool BlockAccepted,
    string? HashHex,
    string? Reason,
    string ShareTargetHex,
    string NetworkTargetHex,
    bool ReloadJob,
    double ShareDifficulty);

public sealed class MiningJobService
{
    private readonly PoolRepository _repository;
    private readonly QadoNodeClient _nodeClient;
    private readonly PoolLogger _logger;

    public MiningJobService(PoolRepository repository, QadoNodeClient nodeClient, PoolLogger logger)
    {
        _repository = repository;
        _nodeClient = nodeClient;
        _logger = logger;
    }

    public async Task<PoolMiningJobEnvelope> CreateMinerJobAsync(MinerRecord miner, PoolSettings settings, CancellationToken cancellationToken = default)
    {
        if (!HexUtility.IsHex(settings.PoolMinerPublicKey, 32))
        {
            throw new InvalidOperationException("Pool miner public key is not configured.");
        }

        var nodeJob = await _nodeClient.CreateMiningJobAsync(settings.PoolMinerPublicKey, cancellationToken).ConfigureAwait(false);
        var round = await _repository.ResolveOpenRoundAsync(
            nodeJob.JobId,
            nodeJob.Height,
            nodeJob.PrevHash,
            nodeJob.Target,
            nodeJob.HeaderHexZeroNonce,
            nodeJob.CoinbaseAmount,
            cancellationToken).ConfigureAwait(false);

        var effectiveDifficulty = DifficultyFixedPoint.ToNormalizedDouble(miner.ShareDifficulty);
        var shareTargetHex = UInt256Utility.ComputeShareTargetHex(effectiveDifficulty, nodeJob.Target);
        var createdUtc = DateTimeOffset.UtcNow;
        var poolJob = new PoolJob(
            Guid.NewGuid().ToString("N"),
            round.RoundId,
            miner.MinerId,
            nodeJob.JobId,
            nodeJob.Height,
            nodeJob.PrevHash,
            nodeJob.Target,
            shareTargetHex,
            nodeJob.HeaderHexZeroNonce,
            nodeJob.Timestamp,
            nodeJob.CoinbaseAmount,
            createdUtc,
            createdUtc.AddSeconds(settings.ShareJobLifetimeSeconds),
            true);

        await _repository.InsertJobAsync(poolJob, cancellationToken).ConfigureAwait(false);
        await _repository.UpdateMinerHeartbeatsAsync(miner.MinerId, createdUtc, null, cancellationToken).ConfigureAwait(false);

        return new PoolMiningJobEnvelope(poolJob, nodeJob);
    }
}

public sealed class ShareValidationService
{
    private readonly PoolRepository _repository;
    private readonly QadoNodeClient _nodeClient;
    private readonly RoundAccountingService _roundAccountingService;
    private readonly DifficultyService _difficultyService;
    private readonly PoolLogger _logger;

    public ShareValidationService(PoolRepository repository, QadoNodeClient nodeClient, RoundAccountingService roundAccountingService, DifficultyService difficultyService, PoolLogger logger)
    {
        _repository = repository;
        _nodeClient = nodeClient;
        _roundAccountingService = roundAccountingService;
        _difficultyService = difficultyService;
        _logger = logger;
    }

    public async Task<ShareSubmissionOutcome> SubmitShareAsync(MinerRecord miner, string poolJobId, string nonceText, string? timestampText, PoolSettings settings, CancellationToken cancellationToken = default)
    {
        var job = await _repository.GetJobByIdAsync(poolJobId, cancellationToken).ConfigureAwait(false);
        var now = DateTimeOffset.UtcNow;

        if (job is null || !job.IsActive || !string.Equals(job.MinerId, miner.MinerId, StringComparison.Ordinal))
        {
            return new ShareSubmissionOutcome(false, false, true, false, false, null, "stale_job", "", "", true, DifficultyFixedPoint.ToNormalizedDouble(miner.ShareDifficulty));
        }

        var openRound = await _repository.GetOpenRoundAsync(cancellationToken).ConfigureAwait(false);
        if (openRound is null || openRound.RoundId != job.RoundId || job.ExpiresUtc <= now)
        {
            await _repository.MarkJobInactiveAsync(job.PoolJobId, cancellationToken).ConfigureAwait(false);
            await RecordRejectedShareAsync(job, miner, nonceText, timestampText ?? job.BaseTimestampText, "stale_job", ShareStatus.Stale, cancellationToken).ConfigureAwait(false);
            return new ShareSubmissionOutcome(false, false, true, false, false, null, "stale_job", job.ShareTargetHex, job.NetworkTargetHex, true, DifficultyFixedPoint.ToNormalizedDouble(miner.ShareDifficulty));
        }

        if (!ulong.TryParse(nonceText, NumberStyles.None, CultureInfo.InvariantCulture, out var nonce))
        {
            await RecordRejectedShareAsync(job, miner, nonceText, timestampText ?? job.BaseTimestampText, "invalid_nonce", ShareStatus.Invalid, cancellationToken).ConfigureAwait(false);
            return new ShareSubmissionOutcome(false, false, false, false, false, null, "invalid_nonce", job.ShareTargetHex, job.NetworkTargetHex, false, DifficultyFixedPoint.ToNormalizedDouble(miner.ShareDifficulty));
        }

        var effectiveTimestampText = string.IsNullOrWhiteSpace(timestampText) ? job.BaseTimestampText : timestampText.Trim();
        if (!ulong.TryParse(effectiveTimestampText, NumberStyles.None, CultureInfo.InvariantCulture, out var timestamp))
        {
            await RecordRejectedShareAsync(job, miner, nonceText, effectiveTimestampText, "invalid_timestamp", ShareStatus.Invalid, cancellationToken).ConfigureAwait(false);
            return new ShareSubmissionOutcome(false, false, false, false, false, null, "invalid_timestamp", job.ShareTargetHex, job.NetworkTargetHex, false, DifficultyFixedPoint.ToNormalizedDouble(miner.ShareDifficulty));
        }

        if (!ulong.TryParse(job.BaseTimestampText, NumberStyles.None, CultureInfo.InvariantCulture, out var baseTimestamp) || timestamp < baseTimestamp)
        {
            await RecordRejectedShareAsync(job, miner, nonceText, effectiveTimestampText, "invalid_timestamp", ShareStatus.Invalid, cancellationToken).ConfigureAwait(false);
            return new ShareSubmissionOutcome(false, false, false, false, false, null, "invalid_timestamp", job.ShareTargetHex, job.NetworkTargetHex, false, DifficultyFixedPoint.ToNormalizedDouble(miner.ShareDifficulty));
        }

        var maxFutureTimestamp = DateTimeOffset.UtcNow.AddHours(2).ToUnixTimeSeconds();
        if (timestamp > (ulong)maxFutureTimestamp)
        {
            await RecordRejectedShareAsync(job, miner, nonceText, effectiveTimestampText, "future_timestamp", ShareStatus.Invalid, cancellationToken).ConfigureAwait(false);
            return new ShareSubmissionOutcome(false, false, false, false, false, null, "future_timestamp", job.ShareTargetHex, job.NetworkTargetHex, false, DifficultyFixedPoint.ToNormalizedDouble(miner.ShareDifficulty));
        }

        var headerBytes = BuildHeader(job.HeaderHexZeroNonce, timestamp, nonce);
        var hash = new byte[32];
        Hasher.Hash(headerBytes, hash);
        var hashHex = HexUtility.ToLowerHex(hash);
        var effectiveDifficulty = DifficultyFixedPoint.ToNormalizedDouble(miner.ShareDifficulty);

        if (!UInt256Utility.IsHashAtOrBelowTarget(hash, job.ShareTargetHex))
        {
            await RecordRejectedShareAsync(job, miner, nonceText, effectiveTimestampText, "low_diff", ShareStatus.Invalid, hashHex, cancellationToken).ConfigureAwait(false);
            return new ShareSubmissionOutcome(false, false, false, false, false, hashHex, "low_diff", job.ShareTargetHex, job.NetworkTargetHex, false, effectiveDifficulty);
        }

        var isBlockCandidate = UInt256Utility.IsHashAtOrBelowTarget(hash, job.NetworkTargetHex);
        var share = new ShareRecord(
            0,
            job.RoundId,
            job.PoolJobId,
            miner.MinerId,
            nonceText,
            effectiveTimestampText,
            hashHex,
            effectiveDifficulty,
            isBlockCandidate ? ShareStatus.BlockCandidate : ShareStatus.Accepted,
            isBlockCandidate,
            now);

        var inserted = await _repository.InsertShareAsync(share, cancellationToken).ConfigureAwait(false);
        if (!inserted)
        {
            return new ShareSubmissionOutcome(false, true, false, isBlockCandidate, false, hashHex, "duplicate_share", job.ShareTargetHex, job.NetworkTargetHex, false, effectiveDifficulty);
        }

        await _repository.UpdateMinerHeartbeatsAsync(miner.MinerId, null, now, cancellationToken).ConfigureAwait(false);

        if (!isBlockCandidate)
        {
            var retarget = await _difficultyService.RebalanceMinerAsync(miner.MinerId, settings, DifficultyRebalanceTrigger.AcceptedShare, cancellationToken).ConfigureAwait(false);
            return new ShareSubmissionOutcome(true, false, false, false, false, hashHex, null, job.ShareTargetHex, job.NetworkTargetHex, retarget.ReloadJob, retarget.ShareDifficulty);
        }

        var submit = await _nodeClient.SubmitMiningAsync(job.NodeJobId, nonceText, effectiveTimestampText, cancellationToken).ConfigureAwait(false);
        await _repository.MarkJobInactiveAsync(job.PoolJobId, cancellationToken).ConfigureAwait(false);

        if (!submit.Accepted)
        {
            _logger.Warn("Mining", $"Node rejected block candidate round={job.RoundId} reason={submit.Reason ?? "unknown"}");
            return new ShareSubmissionOutcome(true, false, false, true, false, hashHex, submit.Reason ?? "submit_rejected", job.ShareTargetHex, job.NetworkTargetHex, true, effectiveDifficulty);
        }

        await _roundAccountingService.CreditAcceptedBlockAsync(job, miner, submit.Hash ?? hashHex, submit.Height ?? job.HeightText, settings, cancellationToken).ConfigureAwait(false);
        return new ShareSubmissionOutcome(true, false, false, true, true, submit.Hash ?? hashHex, null, job.ShareTargetHex, job.NetworkTargetHex, true, effectiveDifficulty);
    }

    private async Task RecordRejectedShareAsync(PoolJob job, MinerRecord miner, string nonceText, string timestampText, string reason, ShareStatus status, CancellationToken cancellationToken)
    {
        var syntheticHash = HexUtility.HashSha256Hex($"{job.PoolJobId}:{nonceText}:{timestampText}:{reason}");
        await RecordRejectedShareAsync(job, miner, nonceText, timestampText, reason, status, syntheticHash, cancellationToken).ConfigureAwait(false);
    }

    private async Task RecordRejectedShareAsync(PoolJob job, MinerRecord miner, string nonceText, string timestampText, string reason, ShareStatus status, string hashHex, CancellationToken cancellationToken)
    {
        _logger.Warn("Mining", $"Rejected share miner={miner.PublicKeyHex} job={job.PoolJobId} reason={reason}");
        var effectiveDifficulty = DifficultyFixedPoint.ToNormalizedDouble(miner.ShareDifficulty);
        _ = await _repository.InsertShareAsync(
            new ShareRecord(0, job.RoundId, job.PoolJobId, miner.MinerId, nonceText, timestampText, hashHex, effectiveDifficulty, status, false, DateTimeOffset.UtcNow),
            cancellationToken).ConfigureAwait(false);
    }

    private static byte[] BuildHeader(string headerHexZeroNonce, ulong timestamp, ulong nonce)
    {
        var header = Convert.FromHexString(headerHexZeroNonce);
        if (header.Length != 145)
        {
            throw new InvalidOperationException("Qado mining header must be 145 bytes.");
        }

        BinaryPrimitives.WriteUInt64BigEndian(header.AsSpan(65, 8), timestamp);
        BinaryPrimitives.WriteUInt64BigEndian(header.AsSpan(105, 8), nonce);
        return header;
    }
}

public enum DifficultyRebalanceTrigger
{
    Background = 1,
    AcceptedShare = 2
}

public sealed record DifficultyRetargetResult(bool Changed, bool ReloadJob, double PreviousDifficulty, double ShareDifficulty);

public sealed class RoundAccountingService
{
    private readonly PoolRepository _repository;
    private readonly PoolLogger _logger;

    public RoundAccountingService(PoolRepository repository, PoolLogger logger)
    {
        _repository = repository;
        _logger = logger;
    }

    public async Task CreditAcceptedBlockAsync(PoolJob job, MinerRecord finderMiner, string blockHashHex, string heightText, PoolSettings settings, CancellationToken cancellationToken = default)
    {
        if (!long.TryParse(job.CoinbaseAmountText, NumberStyles.None, CultureInfo.InvariantCulture, out var rewardAtomic))
        {
            throw new InvalidOperationException("Coinbase amount does not fit into the pool ledger format.");
        }

        await _repository.ExecuteInTransactionAsync(async (connection, transaction) =>
        {
            await _repository.UpdateRoundStateAsync(job.RoundId, RoundStatus.Won, blockHashHex, DateTimeOffset.UtcNow, connection, transaction, cancellationToken).ConfigureAwait(false);
            await _repository.InsertFoundBlockAsync(
                new FoundBlockRecord(Guid.NewGuid().ToString("N"), job.RoundId, blockHashHex, heightText, job.CoinbaseAmountText, DateTimeOffset.UtcNow),
                connection,
                transaction,
                cancellationToken).ConfigureAwait(false);

            var contributionBuckets = await _repository.GetRoundContributionWeightBucketsAsync(job.RoundId, connection, transaction, cancellationToken).ConfigureAwait(false);
            if (contributionBuckets.Count == 0)
            {
                _logger.Warn("Mining", $"Block {blockHashHex} had no contributable shares in round {job.RoundId}.");
                return;
            }

            var contributionWeights = new Dictionary<string, BigInteger>(StringComparer.Ordinal);
            foreach (var bucket in contributionBuckets)
            {
                var scaledDifficulty = bucket.difficultyScaled ?? DifficultyFixedPoint.ToScaled(bucket.difficulty);
                if (scaledDifficulty <= 0 || bucket.shareCount <= 0)
                {
                    continue;
                }

                var bucketWeight = new BigInteger(scaledDifficulty) * bucket.shareCount;
                if (contributionWeights.TryGetValue(bucket.minerId, out var existingWeight))
                {
                    contributionWeights[bucket.minerId] = existingWeight + bucketWeight;
                }
                else
                {
                    contributionWeights[bucket.minerId] = bucketWeight;
                }
            }

            var totalWeight = contributionWeights.Values.Aggregate(BigInteger.Zero, (current, value) => current + value);
            if (totalWeight <= 0)
            {
                _logger.Warn("Mining", $"Block {blockHashHex} had non-positive total share weight in round {job.RoundId}.");
                return;
            }

            var feeAtomic = rewardAtomic * settings.PoolFeeBasisPoints / 10_000L;
            var distributableAtomic = rewardAtomic - feeAtomic;
            var payouts = LargestRemainderAllocation.Allocate(
                distributableAtomic,
                contributionWeights.Select(item => (item.Key, item.Value)));

            foreach (var payout in payouts)
            {
                var miner = await _repository.GetMinerByIdAsync(payout.participantId, connection, transaction, cancellationToken).ConfigureAwait(false);
                if (miner is null)
                {
                    continue;
                }

                await _repository.UpdateBalanceAsync(miner.UserId, payout.amountAtomic, 0, payout.amountAtomic, 0, 0, connection, transaction, cancellationToken).ConfigureAwait(false);
                await _repository.InsertLedgerEntryAsync(
                    new LedgerEntryRecord(Guid.NewGuid().ToString("N"), miner.UserId, LedgerEntryType.BlockReward, payout.amountAtomic, $"round:{job.RoundId}", JsonSerializer.Serialize(new { block = blockHashHex, height = heightText }), DateTimeOffset.UtcNow),
                    connection,
                    transaction,
                    cancellationToken).ConfigureAwait(false);
            }

            _logger.Info("Mining", $"Accepted pool block {blockHashHex} for round {job.RoundId}. Reward={rewardAtomic} fee={feeAtomic} credited={distributableAtomic} payout_mode=largest_remainder.");
        }, cancellationToken).ConfigureAwait(false);
    }
}

public sealed class DifficultyService
{
    private readonly PoolRepository _repository;
    private static readonly TimeSpan InactiveFreezeWindow = TimeSpan.FromMinutes(2);

    public DifficultyService(PoolRepository repository)
    {
        _repository = repository;
    }

    public async Task RebalanceAsync(PoolSettings settings, CancellationToken cancellationToken = default)
    {
        var miners = await _repository.ListVerifiedMinersAsync(cancellationToken).ConfigureAwait(false);
        var now = DateTimeOffset.UtcNow;

        foreach (var miner in miners)
        {
            _ = await RebalanceMinerAsync(miner, settings, DifficultyRebalanceTrigger.Background, now, cancellationToken).ConfigureAwait(false);
        }
    }

    public async Task<DifficultyRetargetResult> RebalanceMinerAsync(string minerId, PoolSettings settings, DifficultyRebalanceTrigger trigger, CancellationToken cancellationToken = default)
    {
        var miner = await _repository.GetMinerByIdAsync(minerId, cancellationToken).ConfigureAwait(false);
        if (miner is null || !miner.IsVerified)
        {
            return new DifficultyRetargetResult(false, false, 1d, 1d);
        }

        return await RebalanceMinerAsync(miner, settings, trigger, DateTimeOffset.UtcNow, cancellationToken).ConfigureAwait(false);
    }

    public async Task<MinerStatsSnapshot> GetMinerStatsAsync(MinerRecord miner, CancellationToken cancellationToken = default)
    {
        var openRound = await _repository.GetOpenRoundAsync(cancellationToken).ConfigureAwait(false);
        var summary = openRound is null
            ? (accepted: 0, stale: 0, invalid: 0)
            : await _repository.GetMinerRoundShareSummaryAsync(miner.MinerId, openRound.RoundId, cancellationToken).ConfigureAwait(false);

        var sampleSince = DateTimeOffset.UtcNow.AddMinutes(-10);
        var acceptedShares = await _repository.GetAcceptedSharesSinceAsync(miner.MinerId, sampleSince, cancellationToken).ConfigureAwait(false);
        var windowSeconds = acceptedShares.Count == 0
            ? 600d
            : Math.Max(60d, (DateTimeOffset.UtcNow - acceptedShares.Min(x => x.SubmittedUtc)).TotalSeconds);
        var estimatedHashrate = acceptedShares.Sum(x => x.Difficulty) / windowSeconds;
        var user = await _repository.GetUserByIdAsync(miner.UserId, cancellationToken).ConfigureAwait(false);

        return new MinerStatsSnapshot(
            miner.MinerId,
            miner.PublicKeyHex,
            user?.Username ?? "unknown",
            miner.ShareDifficulty,
            summary.accepted,
            summary.stale,
            summary.invalid,
            openRound?.RoundId.ToString(CultureInfo.InvariantCulture) ?? "0",
            estimatedHashrate.ToString("0.00", CultureInfo.InvariantCulture),
            miner.LastShareUtc);
    }

    private async Task<DifficultyRetargetResult> RebalanceMinerAsync(MinerRecord miner, PoolSettings settings, DifficultyRebalanceTrigger trigger, DateTimeOffset now, CancellationToken cancellationToken)
    {
        var sampleSince = now.AddMinutes(-10);
        var acceptedShares = await _repository.GetAcceptedSharesSinceAsync(miner.MinerId, sampleSince, cancellationToken).ConfigureAwait(false);
        var currentDifficulty = DifficultyFixedPoint.ToNormalizedDouble(miner.ShareDifficulty);
        var newDifficulty = ComputeDesiredDifficulty(miner, acceptedShares, settings, trigger, sampleSince, now, currentDifficulty);
        var normalizedDifficulty = DifficultyFixedPoint.ToNormalizedDouble(newDifficulty);

        if (!ShouldApplyDifficultyChange(currentDifficulty, normalizedDifficulty))
        {
            return new DifficultyRetargetResult(false, false, currentDifficulty, currentDifficulty);
        }

        await _repository.UpdateMinerDifficultyAsync(miner.MinerId, normalizedDifficulty, cancellationToken).ConfigureAwait(false);
        return new DifficultyRetargetResult(true, true, currentDifficulty, normalizedDifficulty);
    }

    private static double ComputeDesiredDifficulty(
        MinerRecord miner,
        IReadOnlyCollection<ShareRecord> acceptedShares,
        PoolSettings settings,
        DifficultyRebalanceTrigger trigger,
        DateTimeOffset sampleSince,
        DateTimeOffset now,
        double currentDifficulty)
    {
        if (!IsMinerActiveRecently(miner, now))
        {
            return currentDifficulty;
        }

        if (acceptedShares.Count == 0)
        {
            var lastProgressUtc = miner.LastShareUtc is null || miner.LastShareUtc.Value < miner.LastJobUtc
                ? miner.LastJobUtc
                : miner.LastShareUtc.Value;
            var secondsSinceProgress = Math.Max(0d, (now - lastProgressUtc).TotalSeconds);
            var downshiftThresholdSeconds = Math.Max(settings.ShareJobLifetimeSeconds, settings.ShareTargetSecondsMax * 4d);
            if (secondsSinceProgress < downshiftThresholdSeconds)
            {
                return currentDifficulty;
            }

            return Math.Max(1d, currentDifficulty * 0.8d);
        }

        var targetShareSeconds = Math.Max(1d, (settings.ShareTargetSecondsMin + settings.ShareTargetSecondsMax) / 2d);
        var estimatedHashrate = ComputeEstimatedHashrate(miner, acceptedShares, sampleSince, now);
        if (estimatedHashrate <= 0d || double.IsNaN(estimatedHashrate) || double.IsInfinity(estimatedHashrate))
        {
            return currentDifficulty;
        }

        var desiredDifficulty = Math.Max(1d, estimatedHashrate * targetShareSeconds);
        var increaseCap = trigger == DifficultyRebalanceTrigger.AcceptedShare && acceptedShares.Count <= 2 ? 128d : trigger == DifficultyRebalanceTrigger.AcceptedShare ? 16d : 8d;
        var decreaseCap = trigger == DifficultyRebalanceTrigger.AcceptedShare ? 0.5d : 0.67d;
        var cappedDifficulty = Math.Clamp(desiredDifficulty, Math.Max(1d, currentDifficulty * decreaseCap), Math.Max(1d, currentDifficulty * increaseCap));
        return DifficultyFixedPoint.ClampNormalizedDouble(cappedDifficulty);
    }

    private static double ComputeEstimatedHashrate(MinerRecord miner, IReadOnlyCollection<ShareRecord> acceptedShares, DateTimeOffset sampleSince, DateTimeOffset now)
    {
        var firstAcceptedUtc = acceptedShares.Min(x => x.SubmittedUtc);
        var observationStart = firstAcceptedUtc;
        if (miner.LastJobUtc >= sampleSince && miner.LastJobUtc < observationStart)
        {
            observationStart = miner.LastJobUtc;
        }

        var windowSeconds = Math.Max(1d, (now - observationStart).TotalSeconds);
        return acceptedShares.Sum(x => x.Difficulty) / windowSeconds;
    }

    private static bool IsMinerActiveRecently(MinerRecord miner, DateTimeOffset now)
    {
        var lastActivityUtc = miner.LastShareUtc is null || miner.LastShareUtc.Value < miner.LastJobUtc
            ? miner.LastJobUtc
            : miner.LastShareUtc.Value;
        return now - lastActivityUtc <= InactiveFreezeWindow;
    }

    private static bool ShouldApplyDifficultyChange(double currentDifficulty, double newDifficulty)
    {
        if (newDifficulty <= 0d || double.IsNaN(newDifficulty) || double.IsInfinity(newDifficulty))
        {
            return false;
        }

        var ratio = Math.Abs(newDifficulty - currentDifficulty) / Math.Max(1d, currentDifficulty);
        return ratio >= 0.1d;
    }
}

internal static class DifficultyAdjustmentMath
{
    public static double ComputeAverageShareSeconds(IReadOnlyCollection<ShareRecord> acceptedShares, DateTimeOffset sampleSince, DateTimeOffset now)
    {
        var observationWindowSeconds = Math.Max(10d, (now - sampleSince).TotalSeconds);
        if (acceptedShares.Count <= 1)
        {
            return observationWindowSeconds;
        }

        var first = acceptedShares.Min(x => x.SubmittedUtc);
        var last = acceptedShares.Max(x => x.SubmittedUtc);
        var spanSeconds = Math.Max(10d, (last - first).TotalSeconds);
        return spanSeconds / (acceptedShares.Count - 1);
    }
}

public sealed class RoundMonitorService
{
    private readonly PoolRepository _repository;
    private readonly QadoNodeClient _nodeClient;

    public RoundMonitorService(PoolRepository repository, QadoNodeClient nodeClient)
    {
        _repository = repository;
        _nodeClient = nodeClient;
    }

    public async Task RefreshOpenRoundAsync(CancellationToken cancellationToken = default)
    {
        var openRound = await _repository.GetOpenRoundAsync(cancellationToken).ConfigureAwait(false);
        if (openRound is null)
        {
            return;
        }

        var tip = await _nodeClient.GetTipAsync(cancellationToken).ConfigureAwait(false);
        if (!string.Equals(tip.Hash, openRound.PrevHashHex, StringComparison.Ordinal))
        {
            await _repository.CloseOpenRoundsNotMatchingPrevHashAsync(tip.Hash, RoundStatus.Closed, cancellationToken).ConfigureAwait(false);
        }
    }
}
