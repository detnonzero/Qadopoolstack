using Microsoft.Data.Sqlite;
using QadoPoolStack.Desktop.Domain;

namespace QadoPoolStack.Desktop.Persistence;

public sealed partial class PoolRepository
{
    public async Task<List<QadoPayContactRecord>> ListQadoPayContactsByUserIdAsync(string userId, CancellationToken cancellationToken = default)
    {
        var items = new List<QadoPayContactRecord>();
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            SELECT
                contact_id,
                user_id,
                label,
                username,
                created_utc
            FROM qado_pay_contacts
            WHERE user_id = $user_id
            ORDER BY label COLLATE NOCASE, created_utc DESC;
            """,
            ("$user_id", userId));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            items.Add(MapQadoPayContact(reader));
        }

        return items;
    }

    public async Task InsertQadoPayContactAsync(QadoPayContactRecord contact, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            INSERT INTO qado_pay_contacts (
                contact_id,
                user_id,
                label,
                username,
                created_utc
            )
            VALUES (
                $contact_id,
                $user_id,
                $label,
                $username,
                $created_utc
            );
            """,
            ("$contact_id", contact.ContactId),
            ("$user_id", contact.UserId),
            ("$label", contact.Label),
            ("$username", contact.Username),
            ("$created_utc", ToDb(contact.CreatedUtc)));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task DeleteQadoPayContactAsync(string contactId, string userId, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            DELETE FROM qado_pay_contacts
            WHERE contact_id = $contact_id
              AND user_id = $user_id;
            """,
            ("$contact_id", contactId),
            ("$user_id", userId));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<List<LedgerEntryRecord>> ListInternalTransferEntriesByUserIdAsync(string userId, int limit = 50, CancellationToken cancellationToken = default)
    {
        var items = new List<LedgerEntryRecord>();
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            SELECT
                ledger_entry_id,
                user_id,
                entry_type,
                delta_atomic,
                reference,
                metadata_json,
                created_utc
            FROM ledger_entries
            WHERE user_id = $user_id
              AND entry_type IN ($outgoing_type, $incoming_type)
            ORDER BY created_utc DESC, ledger_entry_id DESC
            LIMIT $limit;
            """,
            ("$user_id", userId),
            ("$outgoing_type", (int)LedgerEntryType.InternalTransferOut),
            ("$incoming_type", (int)LedgerEntryType.InternalTransferIn),
            ("$limit", limit));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            items.Add(MapLedgerEntry(reader));
        }

        return items;
    }

    public async Task<(long SentAtomic, long ReceivedAtomic)> GetInternalTransferDailyTotalsByUserIdAsync(
        string userId,
        DateTimeOffset dayStartUtc,
        DateTimeOffset dayEndUtc,
        CancellationToken cancellationToken = default)
    {
        long sentAtomic = 0;
        long receivedAtomic = 0;

        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            SELECT
                entry_type,
                COALESCE(SUM(ABS(delta_atomic)), 0)
            FROM ledger_entries
            WHERE user_id = $user_id
              AND entry_type IN ($outgoing_type, $incoming_type)
              AND created_utc >= $day_start_utc
              AND created_utc < $day_end_utc
            GROUP BY entry_type;
            """,
            ("$user_id", userId),
            ("$outgoing_type", (int)LedgerEntryType.InternalTransferOut),
            ("$incoming_type", (int)LedgerEntryType.InternalTransferIn),
            ("$day_start_utc", ToDb(dayStartUtc)),
            ("$day_end_utc", ToDb(dayEndUtc)));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var entryType = (LedgerEntryType)reader.GetInt32(0);
            var totalAtomic = reader.GetInt64(1);

            if (entryType == LedgerEntryType.InternalTransferOut)
            {
                sentAtomic = totalAtomic;
            }
            else if (entryType == LedgerEntryType.InternalTransferIn)
            {
                receivedAtomic = totalAtomic;
            }
        }

        return (sentAtomic, receivedAtomic);
    }

    private static QadoPayContactRecord MapQadoPayContact(SqliteDataReader reader)
    {
        return new QadoPayContactRecord(
            reader.GetString(0),
            reader.GetString(1),
            reader.GetString(2),
            reader.GetString(3),
            ParseDbTime(reader.GetString(4)));
    }
}
