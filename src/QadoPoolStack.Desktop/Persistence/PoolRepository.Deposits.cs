using System.Linq;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using QadoPoolStack.Desktop.Domain;

namespace QadoPoolStack.Desktop.Persistence;

public sealed partial class PoolRepository
{
    public async Task InsertDepositSenderChallengeAsync(DepositSenderChallenge challenge, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            INSERT INTO deposit_sender_challenges (
                challenge_id,
                user_id,
                public_key_hex,
                message,
                created_utc,
                expires_utc,
                consumed_utc
            )
            VALUES (
                $challenge_id,
                $user_id,
                $public_key_hex,
                $message,
                $created_utc,
                $expires_utc,
                $consumed_utc
            );
            """,
            ("$challenge_id", challenge.ChallengeId),
            ("$user_id", challenge.UserId),
            ("$public_key_hex", challenge.PublicKeyHex),
            ("$message", challenge.Message),
            ("$created_utc", ToDb(challenge.CreatedUtc)),
            ("$expires_utc", ToDb(challenge.ExpiresUtc)),
            ("$consumed_utc", challenge.ConsumedUtc is null ? null : ToDb(challenge.ConsumedUtc.Value)));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<DepositSenderChallenge?> GetDepositSenderChallengeByIdAsync(string challengeId, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            SELECT
                challenge_id,
                user_id,
                public_key_hex,
                message,
                created_utc,
                expires_utc,
                consumed_utc
            FROM deposit_sender_challenges
            WHERE challenge_id = $challenge_id
            LIMIT 1;
            """,
            ("$challenge_id", challengeId));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false) ? MapDepositSenderChallenge(reader) : null;
    }

    public async Task ConsumeDepositSenderChallengeAsync(string challengeId, DateTimeOffset consumedUtc, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            UPDATE deposit_sender_challenges
            SET consumed_utc = $consumed_utc
            WHERE challenge_id = $challenge_id;
            """,
            ("$challenge_id", challengeId),
            ("$consumed_utc", ToDb(consumedUtc)));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<VerifiedDepositSender?> GetVerifiedDepositSenderByPublicKeyAsync(string publicKeyHex, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            SELECT
                sender_id,
                user_id,
                public_key_hex,
                verified_utc
            FROM verified_deposit_senders
            WHERE public_key_hex = $public_key_hex
            LIMIT 1;
            """,
            ("$public_key_hex", publicKeyHex));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false) ? MapVerifiedDepositSender(reader) : null;
    }

    public async Task<List<VerifiedDepositSender>> ListVerifiedDepositSendersByUserIdAsync(string userId, CancellationToken cancellationToken = default)
    {
        var items = new List<VerifiedDepositSender>();
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            SELECT
                sender_id,
                user_id,
                public_key_hex,
                verified_utc
            FROM verified_deposit_senders
            WHERE user_id = $user_id
            ORDER BY verified_utc DESC, public_key_hex;
            """,
            ("$user_id", userId));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            items.Add(MapVerifiedDepositSender(reader));
        }

        return items;
    }

    public async Task<List<VerifiedDepositSender>> GetVerifiedDepositSendersByPublicKeysAsync(IReadOnlyCollection<string> publicKeyHexes, CancellationToken cancellationToken = default)
    {
        if (publicKeyHexes.Count == 0)
        {
            return [];
        }

        var parameters = publicKeyHexes
            .Distinct(StringComparer.Ordinal)
            .Select((value, index) => ($"$public_key_{index}", (object?)value))
            .ToArray();

        var placeholders = string.Join(", ", parameters.Select(parameter => parameter.Item1));
        var items = new List<VerifiedDepositSender>();

        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, $"""
            SELECT
                sender_id,
                user_id,
                public_key_hex,
                verified_utc
            FROM verified_deposit_senders
            WHERE public_key_hex IN ({placeholders});
            """,
            parameters);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            items.Add(MapVerifiedDepositSender(reader));
        }

        return items;
    }

    public async Task<List<MinerRecord>> GetVerifiedMinersByPublicKeysAsync(IReadOnlyCollection<string> publicKeyHexes, CancellationToken cancellationToken = default)
    {
        if (publicKeyHexes.Count == 0)
        {
            return [];
        }

        var parameters = publicKeyHexes
            .Distinct(StringComparer.Ordinal)
            .Select((value, index) => ($"$public_key_{index}", (object?)value))
            .ToArray();

        var placeholders = string.Join(", ", parameters.Select(parameter => parameter.Item1));
        var items = new List<MinerRecord>();

        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, $"""
            SELECT
                miner_id,
                user_id,
                public_key_hex,
                share_difficulty,
                api_token_hash_hex,
                is_verified,
                verified_utc,
                last_job_utc,
                last_share_utc
            FROM miners
            WHERE is_verified = 1
              AND public_key_hex IN ({placeholders});
            """,
            parameters);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            items.Add(MapMiner(reader));
        }

        return items;
    }

    public async Task InsertVerifiedDepositSenderAsync(VerifiedDepositSender sender, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            INSERT INTO verified_deposit_senders (
                sender_id,
                user_id,
                public_key_hex,
                verified_utc
            )
            VALUES (
                $sender_id,
                $user_id,
                $public_key_hex,
                $verified_utc
            );
            """,
            ("$sender_id", sender.SenderId),
            ("$user_id", sender.UserId),
            ("$public_key_hex", sender.PublicKeyHex),
            ("$verified_utc", ToDb(sender.VerifiedUtc)));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task SyncVerifiedDepositSenderToMinerKeyAsync(string userId, string publicKeyHex, CancellationToken cancellationToken = default)
    {
        await ExecuteInTransactionAsync(async (connection, transaction) =>
        {
            await using var ownerCommand = CreateCommand(connection, transaction, """
                SELECT user_id
                FROM verified_deposit_senders
                WHERE public_key_hex = $public_key_hex
                LIMIT 1;
                """,
                ("$public_key_hex", publicKeyHex));
            var existingOwner = await ownerCommand.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) as string;
            if (!string.IsNullOrWhiteSpace(existingOwner) && !string.Equals(existingOwner, userId, StringComparison.Ordinal))
            {
                throw new InvalidOperationException("This key is already linked to another account.");
            }

            await using var deleteCommand = CreateCommand(connection, transaction, """
                DELETE FROM verified_deposit_senders
                WHERE user_id = $user_id
                  AND public_key_hex <> $public_key_hex;
                """,
                ("$user_id", userId),
                ("$public_key_hex", publicKeyHex));
            await deleteCommand.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

            await using var upsertCommand = CreateCommand(connection, transaction, """
                INSERT INTO verified_deposit_senders (
                    sender_id,
                    user_id,
                    public_key_hex,
                    verified_utc
                )
                VALUES (
                    $sender_id,
                    $user_id,
                    $public_key_hex,
                    $verified_utc
                )
                ON CONFLICT(public_key_hex) DO UPDATE SET
                    user_id = excluded.user_id,
                    verified_utc = excluded.verified_utc;
                """,
                ("$sender_id", Guid.NewGuid().ToString("N")),
                ("$user_id", userId),
                ("$public_key_hex", publicKeyHex),
                ("$verified_utc", ToDb(DateTimeOffset.UtcNow)));
            await upsertCommand.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }, cancellationToken).ConfigureAwait(false);
    }

    public async Task DeleteVerifiedDepositSenderAsync(string senderId, string userId, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            DELETE FROM verified_deposit_senders
            WHERE sender_id = $sender_id
              AND user_id = $user_id;
            """,
            ("$sender_id", senderId),
            ("$user_id", userId));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<DepositSyncState?> GetDepositSyncStateAsync(string streamKey, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            SELECT
                stream_key,
                address_hex,
                next_cursor,
                tip_height_text,
                updated_utc
            FROM deposit_sync_state
            WHERE stream_key = $stream_key
            LIMIT 1;
            """,
            ("$stream_key", streamKey));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false) ? MapDepositSyncState(reader) : null;
    }

    public async Task UpsertDepositSyncStateAsync(DepositSyncState state, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await UpsertDepositSyncStateAsync(state, connection, null, cancellationToken).ConfigureAwait(false);
    }

    internal async Task UpsertDepositSyncStateAsync(DepositSyncState state, SqliteConnection connection, SqliteTransaction? transaction, CancellationToken cancellationToken = default)
    {
        await using var command = CreateCommand(connection, transaction, """
            INSERT INTO deposit_sync_state (
                stream_key,
                address_hex,
                next_cursor,
                tip_height_text,
                updated_utc
            )
            VALUES (
                $stream_key,
                $address_hex,
                $next_cursor,
                $tip_height_text,
                $updated_utc
            )
            ON CONFLICT(stream_key) DO UPDATE SET
                address_hex = excluded.address_hex,
                next_cursor = excluded.next_cursor,
                tip_height_text = excluded.tip_height_text,
                updated_utc = excluded.updated_utc;
            """,
            ("$stream_key", state.StreamKey),
            ("$address_hex", state.AddressHex),
            ("$next_cursor", state.NextCursor),
            ("$tip_height_text", state.TipHeightText),
            ("$updated_utc", ToDb(state.UpdatedUtc)));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task InsertIncomingDepositEventIfMissingAsync(IncomingDepositEvent depositEvent, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await InsertIncomingDepositEventIfMissingAsync(depositEvent, connection, null, cancellationToken).ConfigureAwait(false);
    }

    internal async Task InsertIncomingDepositEventIfMissingAsync(IncomingDepositEvent depositEvent, SqliteConnection connection, SqliteTransaction? transaction, CancellationToken cancellationToken = default)
    {
        await using var command = CreateCommand(connection, transaction, """
            INSERT OR IGNORE INTO incoming_deposit_events (
                event_id,
                txid,
                status,
                block_height_text,
                block_hash_hex,
                confirmations_text,
                timestamp_utc,
                to_address_hex,
                amount_atomic,
                from_address_hex,
                from_addresses_json,
                tx_index,
                transfer_index,
                observed_utc,
                credited_user_id,
                matched_sender_public_key_hex,
                credited_utc,
                ignored_utc,
                ignore_reason
            )
            VALUES (
                $event_id,
                $txid,
                $status,
                $block_height_text,
                $block_hash_hex,
                $confirmations_text,
                $timestamp_utc,
                $to_address_hex,
                $amount_atomic,
                $from_address_hex,
                $from_addresses_json,
                $tx_index,
                $transfer_index,
                $observed_utc,
                $credited_user_id,
                $matched_sender_public_key_hex,
                $credited_utc,
                $ignored_utc,
                $ignore_reason
            );
            """,
            ("$event_id", depositEvent.EventId),
            ("$txid", depositEvent.TxId),
            ("$status", depositEvent.Status),
            ("$block_height_text", depositEvent.BlockHeightText),
            ("$block_hash_hex", depositEvent.BlockHashHex),
            ("$confirmations_text", depositEvent.ConfirmationsText),
            ("$timestamp_utc", ToDb(depositEvent.TimestampUtc)),
            ("$to_address_hex", depositEvent.ToAddressHex),
            ("$amount_atomic", depositEvent.AmountAtomic),
            ("$from_address_hex", depositEvent.FromAddressHex),
            ("$from_addresses_json", depositEvent.FromAddressesJson),
            ("$tx_index", depositEvent.TxIndex),
            ("$transfer_index", depositEvent.TransferIndex),
            ("$observed_utc", ToDb(depositEvent.ObservedUtc)),
            ("$credited_user_id", depositEvent.CreditedUserId),
            ("$matched_sender_public_key_hex", depositEvent.MatchedSenderPublicKeyHex),
            ("$credited_utc", depositEvent.CreditedUtc is null ? null : ToDb(depositEvent.CreditedUtc.Value)),
            ("$ignored_utc", depositEvent.IgnoredUtc is null ? null : ToDb(depositEvent.IgnoredUtc.Value)),
            ("$ignore_reason", depositEvent.IgnoreReason));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<List<IncomingDepositEvent>> ListPendingIncomingDepositEventsAsync(int limit = 500, CancellationToken cancellationToken = default)
    {
        var items = new List<IncomingDepositEvent>();
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            SELECT
                event_id,
                txid,
                status,
                block_height_text,
                block_hash_hex,
                confirmations_text,
                timestamp_utc,
                to_address_hex,
                amount_atomic,
                from_address_hex,
                from_addresses_json,
                tx_index,
                transfer_index,
                observed_utc,
                credited_user_id,
                matched_sender_public_key_hex,
                credited_utc,
                ignored_utc,
                ignore_reason
            FROM incoming_deposit_events
            WHERE credited_user_id IS NULL
              AND ignored_utc IS NULL
            ORDER BY CAST(block_height_text AS INTEGER), tx_index, transfer_index
            LIMIT $limit;
            """,
            ("$limit", limit));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            items.Add(MapIncomingDepositEvent(reader));
        }

        return items;
    }

    public async Task<List<IncomingDepositEvent>> ListHistoricallyInvalidCreditedIncomingDepositEventsAsync(int limit = 500, CancellationToken cancellationToken = default)
    {
        var items = new List<IncomingDepositEvent>();
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            SELECT
                event_id,
                txid,
                status,
                block_height_text,
                block_hash_hex,
                confirmations_text,
                timestamp_utc,
                to_address_hex,
                amount_atomic,
                from_address_hex,
                from_addresses_json,
                tx_index,
                transfer_index,
                observed_utc,
                credited_user_id,
                matched_sender_public_key_hex,
                credited_utc,
                ignored_utc,
                ignore_reason
            FROM incoming_deposit_events
            WHERE credited_user_id IS NOT NULL
              AND ignored_utc IS NULL
              AND timestamp_utc < (
                  SELECT created_utc
                  FROM users
                  WHERE user_id = incoming_deposit_events.credited_user_id
              )
            ORDER BY timestamp_utc, tx_index, transfer_index
            LIMIT $limit;
            """,
            ("$limit", limit));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            items.Add(MapIncomingDepositEvent(reader));
        }

        return items;
    }

    public async Task IgnoreIncomingDepositEventAsync(string eventId, string reason, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            UPDATE incoming_deposit_events
            SET ignored_utc = $ignored_utc,
                ignore_reason = $ignore_reason
            WHERE event_id = $event_id
              AND credited_user_id IS NULL
              AND ignored_utc IS NULL;
            """,
            ("$event_id", eventId),
            ("$ignored_utc", ToDb(DateTimeOffset.UtcNow)),
            ("$ignore_reason", reason));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool> ReverseHistoricallyInvalidDepositCreditAsync(IncomingDepositEvent depositEvent, string reason, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(depositEvent.CreditedUserId))
        {
            return false;
        }

        return await ExecuteInTransactionAsync(async (connection, transaction) =>
        {
            await using var updateCommand = CreateCommand(connection, transaction, """
                UPDATE incoming_deposit_events
                SET credited_user_id = NULL,
                    matched_sender_public_key_hex = NULL,
                    credited_utc = NULL,
                    ignored_utc = $ignored_utc,
                    ignore_reason = $ignore_reason
                WHERE event_id = $event_id
                  AND credited_user_id = $credited_user_id
                  AND ignored_utc IS NULL;
                """,
                ("$event_id", depositEvent.EventId),
                ("$credited_user_id", depositEvent.CreditedUserId),
                ("$ignored_utc", ToDb(DateTimeOffset.UtcNow)),
                ("$ignore_reason", reason));

            var updated = await updateCommand.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            if (updated != 1)
            {
                return false;
            }

            await UpdateBalanceAsync(
                depositEvent.CreditedUserId,
                -depositEvent.AmountAtomic,
                0,
                0,
                -depositEvent.AmountAtomic,
                0,
                connection,
                transaction,
                cancellationToken).ConfigureAwait(false);

            await using (var deleteLedgerCommand = CreateCommand(connection, transaction, """
                DELETE FROM ledger_entries
                WHERE user_id = $user_id
                  AND entry_type = $entry_type
                  AND reference = $reference;
                """,
                ("$user_id", depositEvent.CreditedUserId),
                ("$entry_type", (int)LedgerEntryType.DepositCredit),
                ("$reference", $"deposit:{depositEvent.EventId}")))
            {
                await deleteLedgerCommand.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            var metadataJson = JsonSerializer.Serialize(new
            {
                eventId = depositEvent.EventId,
                txid = depositEvent.TxId,
                reason,
                originalTimestampUtc = depositEvent.TimestampUtc,
                amountAtomic = depositEvent.AmountAtomic,
                amount = QadoPoolStack.Desktop.Utilities.AmountUtility.FormatAtomic(depositEvent.AmountAtomic)
            });

            await InsertLedgerEntryAsync(
                new LedgerEntryRecord(
                    Guid.NewGuid().ToString("N"),
                    depositEvent.CreditedUserId,
                    LedgerEntryType.ManualAdjustment,
                    -depositEvent.AmountAtomic,
                    $"deposit-reverse:{depositEvent.EventId}",
                    metadataJson,
                    DateTimeOffset.UtcNow),
                connection,
                transaction,
                cancellationToken).ConfigureAwait(false);

            return true;
        }, cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool> TryCreditIncomingDepositAsync(IncomingDepositEvent depositEvent, string userId, string matchedSenderPublicKeyHex, CancellationToken cancellationToken = default)
    {
        return await ExecuteInTransactionAsync(async (connection, transaction) =>
        {
            await using var updateCommand = CreateCommand(connection, transaction, """
                UPDATE incoming_deposit_events
                SET credited_user_id = $credited_user_id,
                    matched_sender_public_key_hex = $matched_sender_public_key_hex,
                    credited_utc = $credited_utc
                WHERE event_id = $event_id
                  AND credited_user_id IS NULL
                  AND ignored_utc IS NULL;
                """,
                ("$event_id", depositEvent.EventId),
                ("$credited_user_id", userId),
                ("$matched_sender_public_key_hex", matchedSenderPublicKeyHex),
                ("$credited_utc", ToDb(DateTimeOffset.UtcNow)));

            var updated = await updateCommand.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            if (updated != 1)
            {
                return false;
            }

            await UpdateBalanceAsync(userId, depositEvent.AmountAtomic, 0, 0, depositEvent.AmountAtomic, 0, connection, transaction, cancellationToken).ConfigureAwait(false);

            var metadataJson = JsonSerializer.Serialize(new
            {
                eventId = depositEvent.EventId,
                txid = depositEvent.TxId,
                fromAddress = depositEvent.FromAddressHex,
                fromAddresses = JsonSerializer.Deserialize<string[]>(depositEvent.FromAddressesJson) ?? [],
                toAddress = depositEvent.ToAddressHex,
                blockHeight = depositEvent.BlockHeightText,
                blockHash = depositEvent.BlockHashHex,
                confirmations = depositEvent.ConfirmationsText,
                timestampUtc = depositEvent.TimestampUtc,
                matchedSenderPublicKey = matchedSenderPublicKeyHex
            });

            await InsertLedgerEntryAsync(
                new LedgerEntryRecord(
                    Guid.NewGuid().ToString("N"),
                    userId,
                    LedgerEntryType.DepositCredit,
                    depositEvent.AmountAtomic,
                    $"deposit:{depositEvent.EventId}",
                    metadataJson,
                    DateTimeOffset.UtcNow),
                connection,
                transaction,
                cancellationToken).ConfigureAwait(false);

            return true;
        }, cancellationToken).ConfigureAwait(false);
    }

    private static DepositSenderChallenge MapDepositSenderChallenge(SqliteDataReader reader)
    {
        return new DepositSenderChallenge(
            reader.GetString(0),
            reader.GetString(1),
            reader.GetString(2),
            reader.GetString(3),
            ParseDbTime(reader.GetString(4)),
            ParseDbTime(reader.GetString(5)),
            reader.IsDBNull(6) ? null : ParseDbTime(reader.GetString(6)));
    }

    private static VerifiedDepositSender MapVerifiedDepositSender(SqliteDataReader reader)
    {
        return new VerifiedDepositSender(
            reader.GetString(0),
            reader.GetString(1),
            reader.GetString(2),
            ParseDbTime(reader.GetString(3)));
    }

    private static DepositSyncState MapDepositSyncState(SqliteDataReader reader)
    {
        return new DepositSyncState(
            reader.GetString(0),
            reader.GetString(1),
            reader.GetString(2),
            reader.GetString(3),
            ParseDbTime(reader.GetString(4)));
    }

    private static IncomingDepositEvent MapIncomingDepositEvent(SqliteDataReader reader)
    {
        return new IncomingDepositEvent(
            reader.GetString(0),
            reader.GetString(1),
            reader.GetString(2),
            reader.GetString(3),
            reader.GetString(4),
            reader.GetString(5),
            ParseDbTime(reader.GetString(6)),
            reader.GetString(7),
            reader.GetInt64(8),
            reader.IsDBNull(9) ? null : reader.GetString(9),
            reader.GetString(10),
            reader.GetInt32(11),
            reader.GetInt32(12),
            ParseDbTime(reader.GetString(13)),
            reader.IsDBNull(14) ? null : reader.GetString(14),
            reader.IsDBNull(15) ? null : reader.GetString(15),
            reader.IsDBNull(16) ? null : ParseDbTime(reader.GetString(16)),
            reader.IsDBNull(17) ? null : ParseDbTime(reader.GetString(17)),
            reader.IsDBNull(18) ? null : reader.GetString(18));
    }
}
