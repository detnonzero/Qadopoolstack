using System.Globalization;
using System.Text.Json;
using QadoPoolStack.Desktop.Configuration;
using QadoPoolStack.Desktop.Domain;
using QadoPoolStack.Desktop.Infrastructure.Logging;
using QadoPoolStack.Desktop.Persistence;
using QadoPoolStack.Desktop.Services.Node;
using QadoPoolStack.Desktop.Utilities;

namespace QadoPoolStack.Desktop.Services.Accounts;

public sealed class DepositMonitorService
{
    private readonly PoolRepository _repository;
    private readonly QadoNodeClient _nodeClient;
    private readonly PoolLogger _logger;
    private readonly PoolSettings _settings;
    private readonly HashSet<string> _ambiguousEventIdsLogged = new(StringComparer.Ordinal);
    private readonly HashSet<string> _historicalEventIdsLogged = new(StringComparer.Ordinal);
    private bool _missingPoolAddressLogged;

    public DepositMonitorService(PoolRepository repository, QadoNodeClient nodeClient, PoolLogger logger, PoolSettings settings)
    {
        _repository = repository;
        _nodeClient = nodeClient;
        _logger = logger;
        _settings = settings;
    }

    public async Task PollAsync(CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(_settings.PoolMinerPublicKey))
        {
            if (!_missingPoolAddressLogged)
            {
                _missingPoolAddressLogged = true;
                _logger.Warn("Deposit", "Deposit polling is idle because no pool public key is configured.");
            }

            return;
        }

        _missingPoolAddressLogged = false;
        var poolAddress = HexUtility.NormalizeLower(_settings.PoolMinerPublicKey, 32);
        var streamKey = $"incoming:{poolAddress}";
        var existingState = await _repository.GetDepositSyncStateAsync(streamKey, cancellationToken).ConfigureAwait(false);

        var response = await _nodeClient.GetIncomingAddressEventsAsync(
            poolAddress,
            existingState?.NextCursor,
            200,
            _settings.DepositMinConfirmations,
            cancellationToken).ConfigureAwait(false);

        await _repository.ExecuteInTransactionAsync(async (connection, transaction) =>
        {
            foreach (var item in response.Items)
            {
                var depositEvent = MapIncomingDepositEvent(item, DateTimeOffset.UtcNow);
                await _repository.InsertIncomingDepositEventIfMissingAsync(depositEvent, connection, transaction, cancellationToken).ConfigureAwait(false);
            }

            var nextCursor = string.IsNullOrWhiteSpace(response.NextCursor)
                ? existingState?.NextCursor ?? string.Empty
                : response.NextCursor;

            await _repository.UpsertDepositSyncStateAsync(
                new DepositSyncState(
                    streamKey,
                    poolAddress,
                    nextCursor,
                    response.TipHeight,
                    DateTimeOffset.UtcNow),
                connection,
                transaction,
                cancellationToken).ConfigureAwait(false);
        }, cancellationToken).ConfigureAwait(false);

        await ReconcileHistoricallyInvalidCreditsAsync(cancellationToken).ConfigureAwait(false);
        await AttributePendingDepositsAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task ReconcileHistoricallyInvalidCreditsAsync(CancellationToken cancellationToken)
    {
        var invalidCredits = await _repository.ListHistoricallyInvalidCreditedIncomingDepositEventsAsync(200, cancellationToken).ConfigureAwait(false);
        foreach (var invalidCredit in invalidCredits)
        {
            var reversed = await _repository.ReverseHistoricallyInvalidDepositCreditAsync(
                invalidCredit,
                "predates-user-account",
                cancellationToken).ConfigureAwait(false);

            if (reversed)
            {
                _logger.Warn(
                    "Deposit",
                    $"Reversed deposit event {invalidCredit.EventId} because it predates the credited account creation timestamp.");
            }
        }
    }

    private async Task AttributePendingDepositsAsync(CancellationToken cancellationToken)
    {
        var pendingEvents = await _repository.ListPendingIncomingDepositEventsAsync(500, cancellationToken).ConfigureAwait(false);
        if (pendingEvents.Count == 0)
        {
            return;
        }

        var candidateAddresses = pendingEvents
            .SelectMany(GetSenderCandidates)
            .Distinct(StringComparer.Ordinal)
            .ToArray();

        if (candidateAddresses.Length == 0)
        {
            return;
        }

        var verifiedSenders = await _repository.GetVerifiedDepositSendersByPublicKeysAsync(candidateAddresses, cancellationToken).ConfigureAwait(false);
        if (verifiedSenders.Count == 0)
        {
            return;
        }

        var users = await _repository.GetUsersByIdsAsync(verifiedSenders.Select(sender => sender.UserId).Distinct(StringComparer.Ordinal).ToArray(), cancellationToken).ConfigureAwait(false);
        if (users.Count == 0)
        {
            return;
        }

        var userLookup = users.ToDictionary(user => user.UserId, StringComparer.Ordinal);
        var senderLookup = verifiedSenders
            .Where(sender => userLookup.ContainsKey(sender.UserId))
            .ToDictionary(sender => sender.PublicKeyHex, StringComparer.Ordinal);

        foreach (var pendingEvent in pendingEvents)
        {
            if (!string.Equals(pendingEvent.Status, "confirmed", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            var senderCandidates = GetSenderCandidates(pendingEvent);
            if (senderCandidates.Count == 0)
            {
                continue;
            }

            var matchedSenders = senderCandidates
                .Where(senderLookup.ContainsKey)
                .Select(address => senderLookup[address])
                .DistinctBy(sender => sender.SenderId)
                .ToArray();

            if (matchedSenders.Length == 0)
            {
                continue;
            }

            var eligibleSenders = matchedSenders
                .Where(sender => userLookup.TryGetValue(sender.UserId, out var user) && pendingEvent.TimestampUtc >= user.CreatedUtc)
                .ToArray();

            if (eligibleSenders.Length == 0)
            {
                if (senderCandidates.All(senderLookup.ContainsKey))
                {
                    await IgnoreHistoricalPendingEventAsync(pendingEvent, matchedSenders, userLookup, cancellationToken).ConfigureAwait(false);
                }

                continue;
            }

            var distinctUsers = eligibleSenders
                .Select(sender => sender.UserId)
                .Distinct(StringComparer.Ordinal)
                .ToArray();

            if (distinctUsers.Length != 1)
            {
                LogAmbiguousEvent(pendingEvent, eligibleSenders);
                continue;
            }

            var selectedSender = eligibleSenders[0];
            var credited = await _repository.TryCreditIncomingDepositAsync(
                pendingEvent,
                selectedSender.UserId,
                selectedSender.PublicKeyHex,
                cancellationToken).ConfigureAwait(false);

            if (credited)
            {
                _logger.Info(
                    "Deposit",
                    $"Credited {AmountUtility.FormatAtomic(pendingEvent.AmountAtomic)} QADO from {selectedSender.PublicKeyHex} to user_id={selectedSender.UserId} via event={pendingEvent.EventId}");
            }
        }
    }

    private async Task IgnoreHistoricalPendingEventAsync(
        IncomingDepositEvent pendingEvent,
        IReadOnlyCollection<VerifiedDepositSender> matchedSenders,
        IReadOnlyDictionary<string, PoolUser> userLookup,
        CancellationToken cancellationToken)
    {
        await _repository.IgnoreIncomingDepositEventAsync(
            pendingEvent.EventId,
            "predates-user-account",
            cancellationToken).ConfigureAwait(false);

        if (!_historicalEventIdsLogged.Add(pendingEvent.EventId))
        {
            return;
        }

        var matches = string.Join(
            ", ",
            matchedSenders.Select(sender =>
            {
                var user = userLookup[sender.UserId];
                return $"{user.Username}:{sender.PublicKeyHex}:created={user.CreatedUtc:O}";
            }));

        _logger.Warn(
            "Deposit",
            $"Ignored incoming event {pendingEvent.EventId} because it predates the matched account creation timestamp. event_ts={pendingEvent.TimestampUtc:O}; matches={matches}");
    }

    private void LogAmbiguousEvent(IncomingDepositEvent pendingEvent, IReadOnlyCollection<VerifiedDepositSender> matchedSenders)
    {
        if (!_ambiguousEventIdsLogged.Add(pendingEvent.EventId))
        {
            return;
        }

        var users = string.Join(", ", matchedSenders.Select(sender => $"{sender.UserId}:{sender.PublicKeyHex}"));
        _logger.Warn("Deposit", $"Skipped incoming event {pendingEvent.EventId} because multiple verified users matched its sender set. Matches: {users}");
    }

    private static IncomingDepositEvent MapIncomingDepositEvent(QadoIncomingAddressEvent item, DateTimeOffset observedUtc)
    {
        if (!long.TryParse(item.AmountAtomic, NumberStyles.None, CultureInfo.InvariantCulture, out var amountAtomic))
        {
            throw new InvalidOperationException($"Incoming deposit event {item.EventId} returned invalid amount_atomic '{item.AmountAtomic}'.");
        }

        var normalizedSenders = NormalizeSenderAddresses(item.FromAddress, item.FromAddresses);
        return new IncomingDepositEvent(
            item.EventId,
            item.TxId,
            item.Status,
            item.BlockHeight,
            item.BlockHash,
            item.Confirmations,
            item.TimestampUtc,
            item.ToAddress,
            amountAtomic,
            string.IsNullOrWhiteSpace(item.FromAddress) ? null : HexUtility.NormalizeLower(item.FromAddress, 32),
            JsonSerializer.Serialize(normalizedSenders),
            item.TxIndex,
            item.TransferIndex,
            observedUtc,
            null,
            null,
            null,
            null,
            null);
    }

    private static List<string> GetSenderCandidates(IncomingDepositEvent depositEvent)
    {
        var candidates = JsonSerializer.Deserialize<List<string>>(depositEvent.FromAddressesJson) ?? [];
        if (!string.IsNullOrWhiteSpace(depositEvent.FromAddressHex))
        {
            candidates.Add(depositEvent.FromAddressHex);
        }

        return candidates
            .Distinct(StringComparer.Ordinal)
            .ToList();
    }

    private static string[] NormalizeSenderAddresses(string? fromAddress, IReadOnlyCollection<string> fromAddresses)
    {
        var normalized = new HashSet<string>(StringComparer.Ordinal);

        if (!string.IsNullOrWhiteSpace(fromAddress))
        {
            normalized.Add(HexUtility.NormalizeLower(fromAddress, 32));
        }

        foreach (var address in fromAddresses)
        {
            if (!string.IsNullOrWhiteSpace(address))
            {
                normalized.Add(HexUtility.NormalizeLower(address, 32));
            }
        }

        return normalized.ToArray();
    }
}
