using System.Globalization;
using QadoPoolStack.Desktop.Domain;
using QadoPoolStack.Desktop.Infrastructure.Logging;
using QadoPoolStack.Desktop.Infrastructure.Security;
using QadoPoolStack.Desktop.Persistence;
using QadoPoolStack.Desktop.Services.Node;
using QadoPoolStack.Desktop.Utilities;

namespace QadoPoolStack.Desktop.Services.Accounts;

public sealed record CustodianWalletSnapshot(
    string? PublicKeyHex,
    long BalanceAtomic,
    int PendingOutgoingCount,
    int PendingIncomingCount,
    IReadOnlyList<CustodianWalletTimelineItem> Transactions,
    IReadOnlyList<WalletContactRecord> Contacts);

public sealed record CustodianWalletTimelineItem(
    string TransactionId,
    string Direction,
    string CounterpartyAddressHex,
    long AmountAtomic,
    long FeeAtomic,
    string? Note,
    string? TxId,
    string Status,
    DateTimeOffset CreatedUtc);

public sealed record CustodianWalletSendResult(
    string TxId,
    string RecipientAddressHex,
    long AmountAtomic,
    long FeeAtomic);

public sealed class CustodianWalletService
{
    private readonly PoolRepository _repository;
    private readonly QadoNodeClient _nodeClient;
    private readonly SecretProtector _secretProtector;
    private readonly PoolLogger _logger;
    private readonly SemaphoreSlim _sendGate = new(1, 1);

    public CustodianWalletService(PoolRepository repository, QadoNodeClient nodeClient, SecretProtector secretProtector, PoolLogger logger)
    {
        _repository = repository;
        _nodeClient = nodeClient;
        _secretProtector = secretProtector;
        _logger = logger;
    }

    public async Task<PoolUser> CreateKeyPairAsync(PoolUser user, CancellationToken cancellationToken = default)
    {
        if (HasCustodianKeyPair(user))
        {
            return user;
        }

        var (privateKeyHex, publicKeyHex) = KeyUtility.GenerateEd25519KeypairHex();
        await _repository.UpdateUserCustodianWalletAsync(
            user.UserId,
            publicKeyHex,
            _secretProtector.Protect(privateKeyHex),
            cancellationToken).ConfigureAwait(false);

        return await _repository.GetUserByIdAsync(user.UserId, cancellationToken).ConfigureAwait(false)
            ?? throw new InvalidOperationException("Wallet owner no longer exists.");
    }

    public async Task<CustodianWalletSnapshot> GetSnapshotAsync(PoolUser user, CancellationToken cancellationToken = default)
    {
        var contactsTask = _repository.ListWalletContactsByUserIdAsync(user.UserId, cancellationToken);
        var storedTransactionsTask = _repository.ListWalletTransactionsByUserIdAsync(user.UserId, 50, cancellationToken);

        if (!HasCustodianKeyPair(user))
        {
            await Task.WhenAll(contactsTask, storedTransactionsTask).ConfigureAwait(false);
            return new CustodianWalletSnapshot(
                null,
                0,
                0,
                0,
                [],
                contactsTask.Result);
        }

        var publicKeyHex = HexUtility.NormalizeLower(user.CustodianPublicKeyHex!, 32);
        var addressState = await _nodeClient.GetAddressAsync(publicKeyHex, cancellationToken).ConfigureAwait(false);
        QadoAddressIncomingResponse? incomingEvents = null;
        if (addressState is not null)
        {
            incomingEvents = await _nodeClient.GetIncomingAddressEventsAsync(publicKeyHex, null, 50, 0, cancellationToken).ConfigureAwait(false);
        }

        await Task.WhenAll(contactsTask, storedTransactionsTask).ConfigureAwait(false);

        var balanceAtomic = addressState is null ? 0 : ParseAtomic(addressState.BalanceAtomic, "wallet balance");
        var items = new List<CustodianWalletTimelineItem>();
        items.AddRange(storedTransactionsTask.Result.Select(MapStoredTransaction));

        if (incomingEvents is not null)
        {
            foreach (var item in incomingEvents.Items)
            {
                items.Add(MapIncomingTransaction(item));
            }
        }

        var transactions = items
            .OrderByDescending(item => item.CreatedUtc)
            .ThenByDescending(item => item.TransactionId, StringComparer.Ordinal)
            .Take(50)
            .ToArray();

        return new CustodianWalletSnapshot(
            publicKeyHex,
            balanceAtomic,
            addressState?.PendingOutgoingCount ?? 0,
            addressState?.PendingIncomingCount ?? 0,
            transactions,
            contactsTask.Result);
    }

    public async Task<CustodianWalletSendResult> SendTransactionAsync(
        PoolUser user,
        string recipientAddressHex,
        long amountAtomic,
        long feeAtomic,
        string? note,
        CancellationToken cancellationToken = default)
    {
        if (!HasCustodianKeyPair(user))
        {
            throw new InvalidOperationException("Create a custodian wallet keypair first.");
        }

        if (amountAtomic <= 0)
        {
            throw new InvalidOperationException("Transaction amount must be positive.");
        }

        if (feeAtomic < 0)
        {
            throw new InvalidOperationException("Transaction fee must not be negative.");
        }

        recipientAddressHex = HexUtility.NormalizeLower(recipientAddressHex, 32);
        var senderPublicKeyHex = HexUtility.NormalizeLower(user.CustodianPublicKeyHex!, 32);
        var privateKeyHex = _secretProtector.TryUnprotect(user.ProtectedCustodianPrivateKeyHex)
            ?? throw new InvalidOperationException("Custodian wallet private key is not available.");

        var derivedPublicKeyHex = KeyUtility.DeriveEd25519PublicKeyHex(privateKeyHex);
        if (!string.Equals(senderPublicKeyHex, derivedPublicKeyHex, StringComparison.Ordinal))
        {
            throw new InvalidOperationException("Stored custodian wallet keypair is inconsistent.");
        }

        await _sendGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var addressState = await _nodeClient.GetAddressAsync(senderPublicKeyHex, cancellationToken).ConfigureAwait(false)
                ?? throw new InvalidOperationException("Custodian wallet address is not known by the node.");

            var balanceAtomic = ParseAtomic(addressState.BalanceAtomic, "wallet balance");
            var totalCostAtomic = checked(amountAtomic + feeAtomic);
            if (balanceAtomic < totalCostAtomic)
            {
                throw new InvalidOperationException(
                    $"Insufficient wallet balance. Required {AmountUtility.FormatAtomic(totalCostAtomic)} QADO, available {AmountUtility.FormatAtomic(balanceAtomic)} QADO.");
            }

            if (!ulong.TryParse(addressState.Nonce, NumberStyles.None, CultureInfo.InvariantCulture, out var confirmedNonce))
            {
                throw new InvalidOperationException("Node returned an invalid wallet nonce.");
            }

            var network = await _nodeClient.GetNetworkAsync(cancellationToken).ConfigureAwait(false);
            if (!uint.TryParse(network.ChainId, NumberStyles.None, CultureInfo.InvariantCulture, out var chainId) || chainId == 0)
            {
                throw new InvalidOperationException("Node returned an invalid chain id.");
            }

            var nextNonce = checked(confirmedNonce + (ulong)Math.Max(0, addressState.PendingOutgoingCount) + 1UL);
            var rawTransactionHex = QadoTransactionUtility.BuildSignedRawTransactionHex(
                chainId,
                privateKeyHex,
                recipientAddressHex,
                checked((ulong)amountAtomic),
                checked((ulong)feeAtomic),
                nextNonce);

            var broadcast = await _nodeClient.BroadcastTransactionAsync(rawTransactionHex, Guid.NewGuid().ToString("N"), cancellationToken).ConfigureAwait(false);
            if (!broadcast.Accepted)
            {
                throw new InvalidOperationException($"Node rejected wallet transaction: {broadcast.Error ?? broadcast.Status ?? "unknown"}.");
            }

            var txId = string.IsNullOrWhiteSpace(broadcast.TxId)
                ? QadoTransactionUtility.ComputeTransactionIdHex(rawTransactionHex)
                : broadcast.TxId!;

            try
            {
                await _repository.InsertWalletTransactionAsync(
                    new WalletTransactionRecord(
                        Guid.NewGuid().ToString("N"),
                        user.UserId,
                        "outgoing",
                        recipientAddressHex,
                        amountAtomic,
                        feeAtomic,
                        NormalizeNote(note),
                        txId,
                        string.IsNullOrWhiteSpace(broadcast.Status) ? "accepted" : broadcast.Status!,
                        DateTimeOffset.UtcNow),
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.Error("Wallet", ex, $"Broadcasted wallet transaction {txId} for user={user.Username}, but local history persistence failed.");
                throw new InvalidOperationException(
                    $"Wallet transaction {txId} was broadcast successfully, but local history persistence failed. Manual reconciliation is required.",
                    ex);
            }

            return new CustodianWalletSendResult(txId, recipientAddressHex, amountAtomic, feeAtomic);
        }
        finally
        {
            _sendGate.Release();
        }
    }

    public async Task<WalletContactRecord> AddAddressBookEntryAsync(
        PoolUser user,
        string label,
        string addressHex,
        CancellationToken cancellationToken = default)
    {
        label = NormalizeLabel(label);
        addressHex = HexUtility.NormalizeLower(addressHex, 32);

        var existing = await _repository.ListWalletContactsByUserIdAsync(user.UserId, cancellationToken).ConfigureAwait(false);
        if (existing.Any(item => string.Equals(item.Label, label, StringComparison.OrdinalIgnoreCase)))
        {
            throw new InvalidOperationException("This address book label already exists.");
        }

        if (existing.Any(item => string.Equals(item.AddressHex, addressHex, StringComparison.Ordinal)))
        {
            throw new InvalidOperationException("This address is already stored in the address book.");
        }

        var contact = new WalletContactRecord(
            Guid.NewGuid().ToString("N"),
            user.UserId,
            label,
            addressHex,
            DateTimeOffset.UtcNow);

        await _repository.InsertWalletContactAsync(contact, cancellationToken).ConfigureAwait(false);
        return contact;
    }

    public Task DeleteAddressBookEntryAsync(PoolUser user, string contactId, CancellationToken cancellationToken = default)
        => _repository.DeleteWalletContactAsync(contactId, user.UserId, cancellationToken);

    private static bool HasCustodianKeyPair(PoolUser user)
        => !string.IsNullOrWhiteSpace(user.CustodianPublicKeyHex) &&
           !string.IsNullOrWhiteSpace(user.ProtectedCustodianPrivateKeyHex);

    private static CustodianWalletTimelineItem MapStoredTransaction(WalletTransactionRecord item)
    {
        return new CustodianWalletTimelineItem(
            item.WalletTransactionId,
            string.IsNullOrWhiteSpace(item.Direction) ? "outgoing" : item.Direction,
            item.CounterpartyAddressHex,
            item.AmountAtomic,
            item.FeeAtomic,
            item.Note,
            item.TxId,
            string.IsNullOrWhiteSpace(item.Status) ? "accepted" : item.Status,
            item.CreatedUtc);
    }

    private static CustodianWalletTimelineItem MapIncomingTransaction(QadoIncomingAddressEvent item)
    {
        var counterparty = !string.IsNullOrWhiteSpace(item.FromAddress)
            ? HexUtility.NormalizeLower(item.FromAddress, 32)
            : item.FromAddresses.FirstOrDefault(address => !string.IsNullOrWhiteSpace(address)) is { } candidate
                ? HexUtility.NormalizeLower(candidate, 32)
                : "-";

        return new CustodianWalletTimelineItem(
            $"incoming:{item.EventId}",
            "incoming",
            counterparty,
            ParseAtomic(item.AmountAtomic, $"incoming amount for {item.EventId}"),
            0,
            null,
            string.IsNullOrWhiteSpace(item.TxId) ? null : item.TxId,
            string.IsNullOrWhiteSpace(item.Status) ? "observed" : item.Status,
            item.TimestampUtc);
    }

    private static long ParseAtomic(string value, string fieldName)
    {
        if (!long.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed) || parsed < 0)
        {
            throw new InvalidOperationException($"Node returned an invalid {fieldName} value.");
        }

        return parsed;
    }

    private static string NormalizeLabel(string label)
    {
        label = (label ?? string.Empty).Trim();
        if (label.Length is < 1 or > 64)
        {
            throw new InvalidOperationException("Address book label must be between 1 and 64 characters.");
        }

        return label;
    }

    private static string? NormalizeNote(string? note)
    {
        var normalized = string.IsNullOrWhiteSpace(note) ? null : note.Trim();
        if (normalized is not null && normalized.Length > 200)
        {
            throw new InvalidOperationException("Note must be at most 200 characters.");
        }

        return normalized;
    }
}
