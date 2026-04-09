using Microsoft.Data.Sqlite;
using QadoPoolStack.Desktop.Domain;

namespace QadoPoolStack.Desktop.Persistence;

public sealed partial class PoolRepository
{
    public async Task UpdateUserCustodianWalletAsync(
        string userId,
        string publicKeyHex,
        string protectedPrivateKeyHex,
        CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            UPDATE users
            SET custodian_public_key_hex = $custodian_public_key_hex,
                protected_custodian_private_key_hex = $protected_custodian_private_key_hex
            WHERE user_id = $user_id;
            """,
            ("$user_id", userId),
            ("$custodian_public_key_hex", publicKeyHex),
            ("$protected_custodian_private_key_hex", protectedPrivateKeyHex));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<List<WalletContactRecord>> ListWalletContactsByUserIdAsync(string userId, CancellationToken cancellationToken = default)
    {
        var items = new List<WalletContactRecord>();
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            SELECT
                contact_id,
                user_id,
                label,
                address_hex,
                created_utc
            FROM wallet_contacts
            WHERE user_id = $user_id
            ORDER BY label COLLATE NOCASE, created_utc DESC;
            """,
            ("$user_id", userId));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            items.Add(MapWalletContact(reader));
        }

        return items;
    }

    public async Task InsertWalletContactAsync(WalletContactRecord contact, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            INSERT INTO wallet_contacts (
                contact_id,
                user_id,
                label,
                address_hex,
                created_utc
            )
            VALUES (
                $contact_id,
                $user_id,
                $label,
                $address_hex,
                $created_utc
            );
            """,
            ("$contact_id", contact.ContactId),
            ("$user_id", contact.UserId),
            ("$label", contact.Label),
            ("$address_hex", contact.AddressHex),
            ("$created_utc", ToDb(contact.CreatedUtc)));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task DeleteWalletContactAsync(string contactId, string userId, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            DELETE FROM wallet_contacts
            WHERE contact_id = $contact_id
              AND user_id = $user_id;
            """,
            ("$contact_id", contactId),
            ("$user_id", userId));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<List<WalletTransactionRecord>> ListWalletTransactionsByUserIdAsync(string userId, int limit = 50, CancellationToken cancellationToken = default)
    {
        var items = new List<WalletTransactionRecord>();
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            SELECT
                wallet_transaction_id,
                user_id,
                direction,
                counterparty_address_hex,
                amount_atomic,
                fee_atomic,
                note,
                txid,
                status,
                created_utc
            FROM wallet_transactions
            WHERE user_id = $user_id
            ORDER BY created_utc DESC, wallet_transaction_id DESC
            LIMIT $limit;
            """,
            ("$user_id", userId),
            ("$limit", limit));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            items.Add(MapWalletTransaction(reader));
        }

        return items;
    }

    public async Task InsertWalletTransactionAsync(WalletTransactionRecord transactionRecord, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            INSERT INTO wallet_transactions (
                wallet_transaction_id,
                user_id,
                direction,
                counterparty_address_hex,
                amount_atomic,
                fee_atomic,
                note,
                txid,
                status,
                created_utc
            )
            VALUES (
                $wallet_transaction_id,
                $user_id,
                $direction,
                $counterparty_address_hex,
                $amount_atomic,
                $fee_atomic,
                $note,
                $txid,
                $status,
                $created_utc
            );
            """,
            ("$wallet_transaction_id", transactionRecord.WalletTransactionId),
            ("$user_id", transactionRecord.UserId),
            ("$direction", transactionRecord.Direction),
            ("$counterparty_address_hex", transactionRecord.CounterpartyAddressHex),
            ("$amount_atomic", transactionRecord.AmountAtomic),
            ("$fee_atomic", transactionRecord.FeeAtomic),
            ("$note", transactionRecord.Note),
            ("$txid", transactionRecord.TxId),
            ("$status", transactionRecord.Status),
            ("$created_utc", ToDb(transactionRecord.CreatedUtc)));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static WalletContactRecord MapWalletContact(SqliteDataReader reader)
    {
        return new WalletContactRecord(
            reader.GetString(0),
            reader.GetString(1),
            reader.GetString(2),
            reader.GetString(3),
            ParseDbTime(reader.GetString(4)));
    }

    private static WalletTransactionRecord MapWalletTransaction(SqliteDataReader reader)
    {
        return new WalletTransactionRecord(
            reader.GetString(0),
            reader.GetString(1),
            reader.GetString(2),
            reader.GetString(3),
            reader.GetInt64(4),
            reader.GetInt64(5),
            reader.IsDBNull(6) ? null : reader.GetString(6),
            reader.IsDBNull(7) ? null : reader.GetString(7),
            reader.GetString(8),
            ParseDbTime(reader.GetString(9)));
    }
}
