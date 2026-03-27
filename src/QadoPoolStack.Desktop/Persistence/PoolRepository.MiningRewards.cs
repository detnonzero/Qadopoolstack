using Microsoft.Data.Sqlite;
using QadoPoolStack.Desktop.Domain;

namespace QadoPoolStack.Desktop.Persistence;

public sealed partial class PoolRepository
{
    public async Task<int> BackfillLegacyFoundBlocksAsync(int limit, CancellationToken cancellationToken = default)
    {
        return await ExecuteInTransactionAsync(async (connection, transaction) =>
        {
            var candidates = new List<FoundBlockRecord>();
            await using (var command = CreateCommand(connection, transaction, """
                WITH reward_rounds AS (
                    SELECT
                        r.round_id,
                        r.block_hash_hex,
                        r.height_text,
                        r.coinbase_amount_text,
                        MIN(le.created_utc) AS accepted_utc
                    FROM ledger_entries le
                    INNER JOIN rounds r ON r.round_id = CAST(substr(le.reference, 7) AS INTEGER)
                    LEFT JOIN found_blocks fb ON fb.round_id = r.round_id
                    WHERE le.entry_type = $block_reward_type
                      AND le.delta_atomic > 0
                      AND le.reference LIKE 'round:%'
                      AND r.block_hash_hex IS NOT NULL
                      AND fb.block_id IS NULL
                    GROUP BY r.round_id, r.block_hash_hex, r.height_text, r.coinbase_amount_text
                ),
                won_rounds AS (
                    SELECT
                        r.round_id,
                        r.block_hash_hex,
                        r.height_text,
                        r.coinbase_amount_text,
                        COALESCE(r.closed_utc, r.opened_utc) AS accepted_utc
                    FROM rounds r
                    LEFT JOIN found_blocks fb ON fb.round_id = r.round_id
                    WHERE r.status = $won_status
                      AND r.block_hash_hex IS NOT NULL
                      AND fb.block_id IS NULL
                      AND NOT EXISTS (
                          SELECT 1
                          FROM reward_rounds rr
                          WHERE rr.round_id = r.round_id
                      )
                )
                SELECT
                    round_id,
                    block_hash_hex,
                    height_text,
                    coinbase_amount_text,
                    accepted_utc
                FROM reward_rounds
                UNION ALL
                SELECT
                    round_id,
                    block_hash_hex,
                    height_text,
                    coinbase_amount_text,
                    accepted_utc
                FROM won_rounds
                ORDER BY accepted_utc ASC, round_id ASC
                LIMIT $limit;
                """,
                ("$won_status", (int)RoundStatus.Won),
                ("$block_reward_type", (int)LedgerEntryType.BlockReward),
                ("$limit", limit)))
            await using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    var acceptedUtc = ParseDbTime(reader.GetString(4));
                    candidates.Add(new FoundBlockRecord(
                        Guid.NewGuid().ToString("N"),
                        reader.GetInt64(0),
                        reader.GetString(1),
                        reader.GetString(2),
                        reader.GetString(3),
                        FoundBlockStatus.Pending,
                        "0",
                        acceptedUtc,
                        null,
                        null,
                        null));
                }
            }

            foreach (var candidate in candidates)
            {
                await InsertFoundBlockAsync(candidate, connection, transaction, cancellationToken).ConfigureAwait(false);
            }

            return candidates.Count;
        }, cancellationToken).ConfigureAwait(false);
    }

    public async Task<List<FoundBlockRecord>> ListFoundBlocksForSettlementAsync(int limit, CancellationToken cancellationToken = default)
    {
        var items = new List<FoundBlockRecord>();
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            SELECT
                block_id,
                round_id,
                block_hash_hex,
                height_text,
                reward_atomic_text,
                status,
                confirmations_text,
                accepted_utc,
                last_checked_utc,
                finalized_utc,
                orphaned_utc
            FROM found_blocks
            WHERE status IN ($pending_status, $finalized_status)
            ORDER BY
                CASE WHEN last_checked_utc IS NULL THEN 0 ELSE 1 END ASC,
                COALESCE(last_checked_utc, accepted_utc) ASC,
                accepted_utc ASC
            LIMIT $limit;
            """,
            ("$pending_status", (int)FoundBlockStatus.Pending),
            ("$finalized_status", (int)FoundBlockStatus.Finalized),
            ("$limit", limit));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            items.Add(MapFoundBlock(reader));
        }

        return items;
    }

    public async Task<List<FoundBlockPayoutRecord>> ListFoundBlockPayoutsAsync(string blockId, CancellationToken cancellationToken = default)
    {
        var items = new List<FoundBlockPayoutRecord>();
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            SELECT
                payout_id,
                block_id,
                round_id,
                user_id,
                amount_atomic,
                status,
                created_utc,
                finalized_utc,
                reversed_utc
            FROM found_block_payouts
            WHERE block_id = $block_id
            ORDER BY created_utc ASC;
            """,
            ("$block_id", blockId));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            items.Add(MapFoundBlockPayout(reader));
        }

        return items;
    }

    public async Task<List<LedgerEntryRecord>> ListLegacyBlockRewardEntriesByRoundIdAsync(long roundId, CancellationToken cancellationToken = default)
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
            WHERE entry_type = $entry_type
              AND reference = $reference
            ORDER BY created_utc ASC;
            """,
            ("$entry_type", (int)LedgerEntryType.BlockReward),
            ("$reference", $"round:{roundId}"));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            items.Add(MapLedgerEntry(reader));
        }

        return items;
    }

    public async Task InsertFoundBlockPayoutsAsync(IEnumerable<FoundBlockPayoutRecord> payouts, CancellationToken cancellationToken = default)
    {
        await ExecuteInTransactionAsync(async (connection, transaction) =>
        {
            await InsertFoundBlockPayoutsAsync(payouts, connection, transaction, cancellationToken).ConfigureAwait(false);
        }, cancellationToken).ConfigureAwait(false);
    }

    internal async Task InsertFoundBlockPayoutsAsync(IEnumerable<FoundBlockPayoutRecord> payouts, SqliteConnection connection, SqliteTransaction? transaction, CancellationToken cancellationToken = default)
    {
        foreach (var payout in payouts)
        {
            await using var command = CreateCommand(connection, transaction, """
                INSERT OR IGNORE INTO found_block_payouts (
                    payout_id,
                    block_id,
                    round_id,
                    user_id,
                    amount_atomic,
                    status,
                    created_utc,
                    finalized_utc,
                    reversed_utc
                )
                VALUES (
                    $payout_id,
                    $block_id,
                    $round_id,
                    $user_id,
                    $amount_atomic,
                    $status,
                    $created_utc,
                    $finalized_utc,
                    $reversed_utc
                );
                """,
                ("$payout_id", payout.PayoutId),
                ("$block_id", payout.BlockId),
                ("$round_id", payout.RoundId),
                ("$user_id", payout.UserId),
                ("$amount_atomic", payout.AmountAtomic),
                ("$status", (int)payout.Status),
                ("$created_utc", ToDb(payout.CreatedUtc)),
                ("$finalized_utc", payout.FinalizedUtc is null ? null : ToDb(payout.FinalizedUtc.Value)),
                ("$reversed_utc", payout.ReversedUtc is null ? null : ToDb(payout.ReversedUtc.Value)));
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    public async Task UpdateFoundBlockObservationAsync(string blockId, string confirmationsText, DateTimeOffset checkedUtc, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            UPDATE found_blocks
            SET confirmations_text = $confirmations_text,
                last_checked_utc = $last_checked_utc
            WHERE block_id = $block_id;
            """,
            ("$block_id", blockId),
            ("$confirmations_text", confirmationsText),
            ("$last_checked_utc", ToDb(checkedUtc)));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task MarkFoundBlockCanonicalFinalizedAsync(string blockId, string confirmationsText, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            UPDATE found_blocks
            SET status = $status,
                confirmations_text = $confirmations_text,
                last_checked_utc = $last_checked_utc,
                finalized_utc = COALESCE(finalized_utc, $finalized_utc)
            WHERE block_id = $block_id;
            """,
            ("$block_id", blockId),
            ("$status", (int)FoundBlockStatus.Finalized),
            ("$confirmations_text", confirmationsText),
            ("$last_checked_utc", ToDb(DateTimeOffset.UtcNow)),
            ("$finalized_utc", ToDb(DateTimeOffset.UtcNow)));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task FinalizePendingFoundBlockPayoutsAsync(FoundBlockRecord foundBlock, string confirmationsText, CancellationToken cancellationToken = default)
    {
        await ExecuteInTransactionAsync(async (connection, transaction) =>
        {
            var payouts = await ListFoundBlockPayoutsAsync(foundBlock.BlockId, connection, transaction, cancellationToken).ConfigureAwait(false);
            var now = DateTimeOffset.UtcNow;

            foreach (var payout in payouts.Where(item => item.Status == FoundBlockPayoutStatus.Pending))
            {
                await UpdateBalanceAsync(payout.UserId, payout.AmountAtomic, 0, payout.AmountAtomic, 0, 0, connection, transaction, cancellationToken).ConfigureAwait(false);
                await InsertLedgerEntryAsync(
                    new LedgerEntryRecord(
                        Guid.NewGuid().ToString("N"),
                        payout.UserId,
                        LedgerEntryType.BlockReward,
                        payout.AmountAtomic,
                        $"found-block:{foundBlock.BlockId}",
                        System.Text.Json.JsonSerializer.Serialize(new
                        {
                            block = foundBlock.BlockHashHex,
                            height = foundBlock.HeightText,
                            roundId = foundBlock.RoundId
                        }),
                        now),
                    connection,
                    transaction,
                    cancellationToken).ConfigureAwait(false);
            }

            await using (var payoutCommand = CreateCommand(connection, transaction, """
                UPDATE found_block_payouts
                SET status = $status,
                    finalized_utc = COALESCE(finalized_utc, $finalized_utc)
                WHERE block_id = $block_id
                  AND status = $pending_status;
                """,
                ("$block_id", foundBlock.BlockId),
                ("$status", (int)FoundBlockPayoutStatus.Finalized),
                ("$finalized_utc", ToDb(now)),
                ("$pending_status", (int)FoundBlockPayoutStatus.Pending)))
            {
                await payoutCommand.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            await using (var blockCommand = CreateCommand(connection, transaction, """
                UPDATE found_blocks
                SET status = $status,
                    confirmations_text = $confirmations_text,
                    last_checked_utc = $last_checked_utc,
                    finalized_utc = COALESCE(finalized_utc, $finalized_utc)
                WHERE block_id = $block_id;
                """,
                ("$block_id", foundBlock.BlockId),
                ("$status", (int)FoundBlockStatus.Finalized),
                ("$confirmations_text", confirmationsText),
                ("$last_checked_utc", ToDb(now)),
                ("$finalized_utc", ToDb(now))))
            {
                await blockCommand.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }, cancellationToken).ConfigureAwait(false);
    }

    public async Task MarkFoundBlockOrphanedAsync(FoundBlockRecord foundBlock, string reason, CancellationToken cancellationToken = default)
    {
        await ExecuteInTransactionAsync(async (connection, transaction) =>
        {
            var payouts = await ListFoundBlockPayoutsAsync(foundBlock.BlockId, connection, transaction, cancellationToken).ConfigureAwait(false);
            var now = DateTimeOffset.UtcNow;
            var hadFinalizedPayout = false;

            foreach (var payout in payouts)
            {
                if (payout.Status == FoundBlockPayoutStatus.Finalized)
                {
                    hadFinalizedPayout = true;
                    await UpdateBalanceAsync(payout.UserId, -payout.AmountAtomic, 0, -payout.AmountAtomic, 0, 0, connection, transaction, cancellationToken).ConfigureAwait(false);
                    await InsertLedgerEntryAsync(
                        new LedgerEntryRecord(
                            Guid.NewGuid().ToString("N"),
                            payout.UserId,
                            LedgerEntryType.ManualAdjustment,
                            -payout.AmountAtomic,
                            $"block-reward-reverse:{payout.PayoutId}",
                            System.Text.Json.JsonSerializer.Serialize(new
                            {
                                block = foundBlock.BlockHashHex,
                                height = foundBlock.HeightText,
                                roundId = foundBlock.RoundId,
                                reason
                            }),
                            now),
                        connection,
                        transaction,
                        cancellationToken).ConfigureAwait(false);
                }
            }

            await using (var payoutCommand = CreateCommand(connection, transaction, """
                UPDATE found_block_payouts
                SET status = CASE
                        WHEN status = $finalized_status THEN $reversed_status
                        WHEN status = $pending_status THEN $orphaned_status
                        ELSE status
                    END,
                    reversed_utc = CASE
                        WHEN status = $finalized_status THEN COALESCE(reversed_utc, $reversed_utc)
                        ELSE reversed_utc
                    END
                WHERE block_id = $block_id
                  AND status IN ($pending_status, $finalized_status);
                """,
                ("$block_id", foundBlock.BlockId),
                ("$pending_status", (int)FoundBlockPayoutStatus.Pending),
                ("$finalized_status", (int)FoundBlockPayoutStatus.Finalized),
                ("$orphaned_status", (int)FoundBlockPayoutStatus.Orphaned),
                ("$reversed_status", (int)FoundBlockPayoutStatus.Reversed),
                ("$reversed_utc", ToDb(now))))
            {
                await payoutCommand.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            await using (var roundCommand = CreateCommand(connection, transaction, """
                UPDATE rounds
                SET status = $status,
                    closed_utc = COALESCE(closed_utc, $closed_utc),
                    block_hash_hex = COALESCE($block_hash_hex, block_hash_hex)
                WHERE round_id = $round_id;
                """,
                ("$round_id", foundBlock.RoundId),
                ("$status", (int)RoundStatus.Orphaned),
                ("$closed_utc", ToDb(now)),
                ("$block_hash_hex", foundBlock.BlockHashHex)))
            {
                await roundCommand.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            await using (var blockCommand = CreateCommand(connection, transaction, """
                UPDATE found_blocks
                SET status = $status,
                    confirmations_text = '0',
                    last_checked_utc = $last_checked_utc,
                    orphaned_utc = COALESCE(orphaned_utc, $orphaned_utc)
                WHERE block_id = $block_id;
                """,
                ("$block_id", foundBlock.BlockId),
                ("$status", (int)(hadFinalizedPayout ? FoundBlockStatus.Reversed : FoundBlockStatus.Orphaned)),
                ("$last_checked_utc", ToDb(now)),
                ("$orphaned_utc", ToDb(now))))
            {
                await blockCommand.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }, cancellationToken).ConfigureAwait(false);
    }

    internal async Task<List<FoundBlockPayoutRecord>> ListFoundBlockPayoutsAsync(string blockId, SqliteConnection connection, SqliteTransaction? transaction, CancellationToken cancellationToken = default)
    {
        var items = new List<FoundBlockPayoutRecord>();
        await using var command = CreateCommand(connection, transaction, """
            SELECT
                payout_id,
                block_id,
                round_id,
                user_id,
                amount_atomic,
                status,
                created_utc,
                finalized_utc,
                reversed_utc
            FROM found_block_payouts
            WHERE block_id = $block_id
            ORDER BY created_utc ASC;
            """,
            ("$block_id", blockId));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            items.Add(MapFoundBlockPayout(reader));
        }

        return items;
    }

    private static FoundBlockPayoutRecord MapFoundBlockPayout(SqliteDataReader reader)
    {
        return new FoundBlockPayoutRecord(
            reader.GetString(0),
            reader.GetString(1),
            reader.GetInt64(2),
            reader.GetString(3),
            reader.GetInt64(4),
            (FoundBlockPayoutStatus)reader.GetInt32(5),
            ParseDbTime(reader.GetString(6)),
            reader.IsDBNull(7) ? null : ParseDbTime(reader.GetString(7)),
            reader.IsDBNull(8) ? null : ParseDbTime(reader.GetString(8)));
    }
}
