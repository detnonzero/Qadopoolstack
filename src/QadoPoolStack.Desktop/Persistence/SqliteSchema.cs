using Microsoft.Data.Sqlite;
using QadoPoolStack.Desktop.Utilities;

namespace QadoPoolStack.Desktop.Persistence;

public static class SqliteSchema
{
    public static async Task InitializeAsync(SqliteConnectionFactory connectionFactory, CancellationToken cancellationToken = default)
    {
        await using var connection = await connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TABLE IF NOT EXISTS users (
                user_id TEXT PRIMARY KEY,
                username TEXT NOT NULL UNIQUE,
                password_hash_hex TEXT NOT NULL,
                password_salt_hex TEXT NOT NULL,
                deposit_address_hex TEXT NOT NULL UNIQUE,
                protected_deposit_private_key_hex TEXT NOT NULL,
                withdrawal_address_hex TEXT NULL,
                last_observed_deposit_atomic INTEGER NOT NULL DEFAULT 0,
                last_observed_deposit_height_text TEXT NOT NULL DEFAULT '0',
                created_utc TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sessions (
                session_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
                token_hash_hex TEXT NOT NULL UNIQUE,
                created_utc TEXT NOT NULL,
                expires_utc TEXT NOT NULL,
                last_seen_utc TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS miners (
                miner_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL UNIQUE REFERENCES users(user_id) ON DELETE CASCADE,
                public_key_hex TEXT NOT NULL UNIQUE,
                share_difficulty REAL NOT NULL,
                api_token_hash_hex TEXT NOT NULL UNIQUE,
                is_verified INTEGER NOT NULL,
                verified_utc TEXT NOT NULL,
                last_job_utc TEXT NOT NULL,
                last_share_utc TEXT NULL
            );

            CREATE TABLE IF NOT EXISTS challenges (
                challenge_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
                public_key_hex TEXT NOT NULL,
                message TEXT NOT NULL,
                created_utc TEXT NOT NULL,
                expires_utc TEXT NOT NULL,
                consumed_utc TEXT NULL
            );

            CREATE TABLE IF NOT EXISTS deposit_sender_challenges (
                challenge_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
                public_key_hex TEXT NOT NULL,
                message TEXT NOT NULL,
                created_utc TEXT NOT NULL,
                expires_utc TEXT NOT NULL,
                consumed_utc TEXT NULL
            );

            CREATE TABLE IF NOT EXISTS verified_deposit_senders (
                sender_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
                public_key_hex TEXT NOT NULL UNIQUE,
                verified_utc TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS deposit_sync_state (
                stream_key TEXT PRIMARY KEY,
                address_hex TEXT NOT NULL,
                next_cursor TEXT NOT NULL,
                tip_height_text TEXT NOT NULL,
                updated_utc TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS incoming_deposit_events (
                event_id TEXT PRIMARY KEY,
                txid TEXT NOT NULL,
                status TEXT NOT NULL,
                block_height_text TEXT NOT NULL,
                block_hash_hex TEXT NOT NULL,
                confirmations_text TEXT NOT NULL,
                timestamp_utc TEXT NOT NULL,
                to_address_hex TEXT NOT NULL,
                amount_atomic INTEGER NOT NULL,
                from_address_hex TEXT NULL,
                from_addresses_json TEXT NOT NULL,
                tx_index INTEGER NOT NULL,
                transfer_index INTEGER NOT NULL,
                observed_utc TEXT NOT NULL,
                credited_user_id TEXT NULL REFERENCES users(user_id) ON DELETE SET NULL,
                matched_sender_public_key_hex TEXT NULL,
                credited_utc TEXT NULL,
                ignored_utc TEXT NULL,
                ignore_reason TEXT NULL
            );

            CREATE TABLE IF NOT EXISTS rounds (
                round_id INTEGER PRIMARY KEY AUTOINCREMENT,
                node_job_id TEXT NOT NULL,
                height_text TEXT NOT NULL,
                prev_hash_hex TEXT NOT NULL,
                network_target_hex TEXT NOT NULL,
                header_hex_zero_nonce TEXT NOT NULL,
                coinbase_amount_text TEXT NOT NULL,
                opened_utc TEXT NOT NULL,
                closed_utc TEXT NULL,
                status INTEGER NOT NULL,
                block_hash_hex TEXT NULL
            );

            CREATE TABLE IF NOT EXISTS jobs (
                pool_job_id TEXT PRIMARY KEY,
                round_id INTEGER NOT NULL REFERENCES rounds(round_id) ON DELETE CASCADE,
                miner_id TEXT NOT NULL REFERENCES miners(miner_id) ON DELETE CASCADE,
                node_job_id TEXT NOT NULL,
                height_text TEXT NOT NULL,
                prev_hash_hex TEXT NOT NULL,
                network_target_hex TEXT NOT NULL,
                share_target_hex TEXT NOT NULL,
                header_hex_zero_nonce TEXT NOT NULL,
                base_timestamp_text TEXT NOT NULL,
                coinbase_amount_text TEXT NOT NULL,
                created_utc TEXT NOT NULL,
                expires_utc TEXT NOT NULL,
                is_active INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS shares (
                share_id INTEGER PRIMARY KEY AUTOINCREMENT,
                round_id INTEGER NOT NULL REFERENCES rounds(round_id) ON DELETE CASCADE,
                pool_job_id TEXT NOT NULL REFERENCES jobs(pool_job_id) ON DELETE CASCADE,
                miner_id TEXT NOT NULL REFERENCES miners(miner_id) ON DELETE CASCADE,
                nonce_text TEXT NOT NULL,
                timestamp_text TEXT NOT NULL,
                hash_hex TEXT NOT NULL,
                difficulty REAL NOT NULL,
                status INTEGER NOT NULL,
                meets_block_target INTEGER NOT NULL,
                submitted_utc TEXT NOT NULL,
                UNIQUE(pool_job_id, nonce_text, timestamp_text),
                UNIQUE(pool_job_id, hash_hex)
            );

            CREATE TABLE IF NOT EXISTS found_blocks (
                block_id TEXT PRIMARY KEY,
                round_id INTEGER NOT NULL UNIQUE REFERENCES rounds(round_id) ON DELETE CASCADE,
                block_hash_hex TEXT NOT NULL UNIQUE,
                height_text TEXT NOT NULL,
                reward_atomic_text TEXT NOT NULL,
                accepted_utc TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS balances (
                user_id TEXT PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,
                available_atomic INTEGER NOT NULL,
                pending_withdrawal_atomic INTEGER NOT NULL,
                total_mined_atomic INTEGER NOT NULL,
                total_deposited_atomic INTEGER NOT NULL,
                total_withdrawn_atomic INTEGER NOT NULL,
                updated_utc TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS ledger_entries (
                ledger_entry_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
                entry_type INTEGER NOT NULL,
                delta_atomic INTEGER NOT NULL,
                reference TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                created_utc TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS withdrawal_requests (
                withdrawal_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
                amount_atomic INTEGER NOT NULL,
                destination_address_hex TEXT NOT NULL,
                status INTEGER NOT NULL,
                admin_note TEXT NULL,
                external_txid TEXT NULL,
                requested_utc TEXT NOT NULL,
                updated_utc TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS ix_sessions_token_hash ON sessions(token_hash_hex);
            CREATE INDEX IF NOT EXISTS ix_sessions_expires_utc ON sessions(expires_utc);
            CREATE INDEX IF NOT EXISTS ix_challenges_expires_utc ON challenges(expires_utc);
            CREATE INDEX IF NOT EXISTS ix_deposit_sender_challenges_expires_utc ON deposit_sender_challenges(expires_utc);
            CREATE INDEX IF NOT EXISTS ix_verified_deposit_senders_user_verified ON verified_deposit_senders(user_id, verified_utc);
            CREATE INDEX IF NOT EXISTS ix_incoming_deposit_events_uncredited ON incoming_deposit_events(credited_user_id, block_height_text, tx_index, transfer_index);
            CREATE INDEX IF NOT EXISTS ix_incoming_deposit_events_txid ON incoming_deposit_events(txid);
            CREATE INDEX IF NOT EXISTS ix_jobs_active_expires ON jobs(is_active, expires_utc);
            CREATE INDEX IF NOT EXISTS ix_shares_round_status ON shares(round_id, status);
            CREATE INDEX IF NOT EXISTS ix_shares_round_miner_status ON shares(round_id, miner_id, status);
            CREATE INDEX IF NOT EXISTS ix_rounds_status_opened ON rounds(status, opened_utc);
            CREATE INDEX IF NOT EXISTS ix_ledger_user_created ON ledger_entries(user_id, created_utc);
            CREATE INDEX IF NOT EXISTS ix_withdrawals_status_requested ON withdrawal_requests(status, requested_utc);
            """;
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        await EnsureColumnAsync(connection, "incoming_deposit_events", "ignored_utc", "TEXT NULL", cancellationToken).ConfigureAwait(false);
        await EnsureColumnAsync(connection, "incoming_deposit_events", "ignore_reason", "TEXT NULL", cancellationToken).ConfigureAwait(false);
        await EnsureColumnAsync(connection, "shares", "difficulty_scaled", "INTEGER NULL", cancellationToken).ConfigureAwait(false);
        await BackfillShareDifficultyScaledAsync(connection, cancellationToken).ConfigureAwait(false);
    }

    private static async Task EnsureColumnAsync(SqliteConnection connection, string tableName, string columnName, string columnDefinition, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = $"PRAGMA table_info({tableName});";
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (string.Equals(reader.GetString(1), columnName, StringComparison.OrdinalIgnoreCase))
            {
                return;
            }
        }

        await reader.DisposeAsync().ConfigureAwait(false);

        await using var alterCommand = connection.CreateCommand();
        alterCommand.CommandText = $"ALTER TABLE {tableName} ADD COLUMN {columnName} {columnDefinition};";
        await alterCommand.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static async Task BackfillShareDifficultyScaledAsync(SqliteConnection connection, CancellationToken cancellationToken)
    {
        const int batchSize = 2048;

        while (true)
        {
            var pending = new List<(long shareId, double difficulty)>(batchSize);

            await using (var selectCommand = connection.CreateCommand())
            {
                selectCommand.CommandText = """
                    SELECT share_id, difficulty
                    FROM shares
                    WHERE difficulty_scaled IS NULL
                    ORDER BY share_id
                    LIMIT $limit;
                    """;
                selectCommand.Parameters.AddWithValue("$limit", batchSize);

                await using var reader = await selectCommand.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    pending.Add((reader.GetInt64(0), reader.GetDouble(1)));
                }
            }

            if (pending.Count == 0)
            {
                return;
            }

            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            await using var updateCommand = connection.CreateCommand();
            updateCommand.Transaction = transaction;
            updateCommand.CommandText = """
                UPDATE shares
                SET difficulty_scaled = $difficulty_scaled
                WHERE share_id = $share_id;
                """;

            var difficultyScaledParameter = updateCommand.Parameters.Add("$difficulty_scaled", SqliteType.Integer);
            var shareIdParameter = updateCommand.Parameters.Add("$share_id", SqliteType.Integer);

            foreach (var item in pending)
            {
                difficultyScaledParameter.Value = DifficultyFixedPoint.ToScaled(item.difficulty);
                shareIdParameter.Value = item.shareId;
                await updateCommand.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
