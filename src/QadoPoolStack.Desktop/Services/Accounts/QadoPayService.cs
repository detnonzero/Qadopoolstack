using QadoPoolStack.Desktop.Domain;
using QadoPoolStack.Desktop.Persistence;

namespace QadoPoolStack.Desktop.Services.Accounts;

public sealed record QadoPaySnapshot(
    IReadOnlyList<QadoPayContactRecord> Contacts,
    IReadOnlyList<LedgerEntryRecord> Payments,
    QadoPayOverview Overview);

public sealed record QadoPayOverview(
    long SentTodayAtomic,
    long ReceivedTodayAtomic,
    LedgerEntryRecord? LastPayment);

public sealed class QadoPayService
{
    private readonly PoolRepository _repository;

    public QadoPayService(PoolRepository repository)
    {
        _repository = repository;
    }

    public async Task<QadoPaySnapshot> GetSnapshotAsync(PoolUser user, CancellationToken cancellationToken = default)
    {
        var contactsTask = _repository.ListQadoPayContactsByUserIdAsync(user.UserId, cancellationToken);
        var paymentsTask = _repository.ListInternalTransferEntriesByUserIdAsync(user.UserId, 50, cancellationToken);
        var (dayStartUtc, dayEndUtc) = GetLocalDayBoundsUtc(DateTimeOffset.UtcNow);
        var totalsTask = _repository.GetInternalTransferDailyTotalsByUserIdAsync(user.UserId, dayStartUtc, dayEndUtc, cancellationToken);
        await Task.WhenAll(contactsTask, paymentsTask, totalsTask).ConfigureAwait(false);

        var payments = paymentsTask.Result;
        var (sentTodayAtomic, receivedTodayAtomic) = totalsTask.Result;

        return new QadoPaySnapshot(
            contactsTask.Result,
            payments,
            new QadoPayOverview(
                sentTodayAtomic,
                receivedTodayAtomic,
                payments.FirstOrDefault()));
    }

    public async Task<QadoPayContactRecord> AddAddressBookEntryAsync(
        PoolUser user,
        string label,
        string username,
        CancellationToken cancellationToken = default)
    {
        label = NormalizeLabel(label);
        username = NormalizeUsername(username);

        if (string.Equals(user.Username, username, StringComparison.Ordinal))
        {
            throw new InvalidOperationException("You cannot save your own username in the Qado Pay address book.");
        }

        var recipient = await _repository.GetUserByUsernameAsync(username, cancellationToken).ConfigureAwait(false);
        if (recipient is null)
        {
            throw new InvalidOperationException("This pool user does not exist.");
        }

        var existing = await _repository.ListQadoPayContactsByUserIdAsync(user.UserId, cancellationToken).ConfigureAwait(false);
        if (existing.Any(item => string.Equals(item.Label, label, StringComparison.OrdinalIgnoreCase)))
        {
            throw new InvalidOperationException("This address book label already exists.");
        }

        if (existing.Any(item => string.Equals(item.Username, username, StringComparison.Ordinal)))
        {
            throw new InvalidOperationException("This username is already stored in the address book.");
        }

        var contact = new QadoPayContactRecord(
            Guid.NewGuid().ToString("N"),
            user.UserId,
            label,
            username,
            DateTimeOffset.UtcNow);

        await _repository.InsertQadoPayContactAsync(contact, cancellationToken).ConfigureAwait(false);
        return contact;
    }

    public Task DeleteAddressBookEntryAsync(PoolUser user, string contactId, CancellationToken cancellationToken = default)
        => _repository.DeleteQadoPayContactAsync(contactId, user.UserId, cancellationToken);

    private static string NormalizeLabel(string label)
    {
        label = (label ?? string.Empty).Trim();
        if (label.Length is < 1 or > 64)
        {
            throw new InvalidOperationException("Address book label must be between 1 and 64 characters.");
        }

        return label;
    }

    private static string NormalizeUsername(string username)
    {
        username = (username ?? string.Empty).Trim().ToLowerInvariant();
        if (username.Length is < 3 or > 32)
        {
            throw new InvalidOperationException("Username must be between 3 and 32 characters.");
        }

        foreach (var c in username)
        {
            if (!(char.IsAsciiLetterOrDigit(c) || c is '_' or '-' or '.'))
            {
                throw new InvalidOperationException("Username contains unsupported characters.");
            }
        }

        return username;
    }

    private static (DateTimeOffset DayStartUtc, DateTimeOffset DayEndUtc) GetLocalDayBoundsUtc(DateTimeOffset utcNow)
    {
        var localZone = TimeZoneInfo.Local;
        var localNow = TimeZoneInfo.ConvertTime(utcNow, localZone);
        var localDate = DateOnly.FromDateTime(localNow.DateTime);

        var startLocalTime = localDate.ToDateTime(TimeOnly.MinValue, DateTimeKind.Unspecified);
        var endLocalTime = localDate.AddDays(1).ToDateTime(TimeOnly.MinValue, DateTimeKind.Unspecified);

        var startLocal = new DateTimeOffset(startLocalTime, localZone.GetUtcOffset(startLocalTime));
        var endLocal = new DateTimeOffset(endLocalTime, localZone.GetUtcOffset(endLocalTime));

        return (startLocal.ToUniversalTime(), endLocal.ToUniversalTime());
    }
}
