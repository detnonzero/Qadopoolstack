using System.Globalization;
using Microsoft.Data.Sqlite;
using QadoPoolStack.Desktop.Domain;
using QadoPoolStack.Desktop.Utilities;

namespace QadoPoolStack.Desktop.Persistence;

public sealed partial class PoolRepository
{
    private readonly SqliteConnectionFactory _connectionFactory;
    private readonly SemaphoreSlim _roundStateGate = new(1, 1);

    public PoolRepository(SqliteConnectionFactory connectionFactory)
    {
        _connectionFactory = connectionFactory;
    }

    public Task InitializeAsync(CancellationToken cancellationToken = default)
        => SqliteSchema.InitializeAsync(_connectionFactory, cancellationToken);

    public async Task<T> ExecuteInTransactionAsync<T>(Func<SqliteConnection, SqliteTransaction, Task<T>> action, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            var result = await action(connection, transaction).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return result;
        }
        catch
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            throw;
        }
    }

    public Task ExecuteInTransactionAsync(Func<SqliteConnection, SqliteTransaction, Task> action, CancellationToken cancellationToken = default)
        => ExecuteInTransactionAsync<object?>(async (connection, transaction) =>
        {
            await action(connection, transaction).ConfigureAwait(false);
            return null;
        }, cancellationToken);

    public async Task InsertUserAsync(PoolUser user, CancellationToken cancellationToken = default)
    {
        await ExecuteInTransactionAsync(async (connection, transaction) =>
        {
            await InsertUserAsync(user, connection, transaction, cancellationToken).ConfigureAwait(false);
            await EnsureBalanceAsync(user.UserId, connection, transaction, cancellationToken).ConfigureAwait(false);
        }, cancellationToken).ConfigureAwait(false);
    }

    internal async Task InsertUserAsync(PoolUser user, SqliteConnection connection, SqliteTransaction transaction, CancellationToken cancellationToken = default)
    {
        await using var command = CreateCommand(connection, transaction, """
            INSERT INTO users (
                user_id,
                username,
                password_hash_hex,
                password_salt_hex,
                deposit_address_hex,
                protected_deposit_private_key_hex,
                withdrawal_address_hex,
                last_observed_deposit_atomic,
                last_observed_deposit_height_text,
                created_utc
            )
            VALUES (
                $user_id,
                $username,
                $password_hash_hex,
                $password_salt_hex,
                $deposit_address_hex,
                $protected_deposit_private_key_hex,
                $withdrawal_address_hex,
                $last_observed_deposit_atomic,
                $last_observed_deposit_height_text,
                $created_utc
            );
            """,
            ("$user_id", user.UserId),
            ("$username", user.Username),
            ("$password_hash_hex", user.PasswordHashHex),
            ("$password_salt_hex", user.PasswordSaltHex),
            ("$deposit_address_hex", user.DepositAddressHex),
            ("$protected_deposit_private_key_hex", user.ProtectedDepositPrivateKeyHex),
            ("$withdrawal_address_hex", user.WithdrawalAddressHex),
            ("$last_observed_deposit_atomic", user.LastObservedDepositAtomic),
            ("$last_observed_deposit_height_text", user.LastObservedDepositHeightText),
            ("$created_utc", ToDb(user.CreatedUtc)));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<PoolUser?> GetUserByUsernameAsync(string username, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        return await GetUserByUsernameAsync(username, connection, null, cancellationToken).ConfigureAwait(false);
    }

    internal async Task<PoolUser?> GetUserByUsernameAsync(string username, SqliteConnection connection, SqliteTransaction? transaction, CancellationToken cancellationToken = default)
    {
        await using var command = CreateCommand(connection, transaction, """
            SELECT
                user_id,
                username,
                password_hash_hex,
                password_salt_hex,
                deposit_address_hex,
                protected_deposit_private_key_hex,
                withdrawal_address_hex,
                last_observed_deposit_atomic,
                last_observed_deposit_height_text,
                created_utc
            FROM users
            WHERE username = $username
            LIMIT 1;
            """,
            ("$username", username));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false) ? MapUser(reader) : null;
    }

    public async Task<PoolUser?> GetUserByIdAsync(string userId, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        return await GetUserByIdAsync(userId, connection, null, cancellationToken).ConfigureAwait(false);
    }

    internal async Task<PoolUser?> GetUserByIdAsync(string userId, SqliteConnection connection, SqliteTransaction? transaction, CancellationToken cancellationToken = default)
    {
        await using var command = CreateCommand(connection, transaction, """
            SELECT
                user_id,
                username,
                password_hash_hex,
                password_salt_hex,
                deposit_address_hex,
                protected_deposit_private_key_hex,
                withdrawal_address_hex,
                last_observed_deposit_atomic,
                last_observed_deposit_height_text,
                created_utc
            FROM users
            WHERE user_id = $user_id
            LIMIT 1;
            """,
            ("$user_id", userId));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false) ? MapUser(reader) : null;
    }

    public async Task<List<PoolUser>> GetUsersByIdsAsync(IReadOnlyCollection<string> userIds, CancellationToken cancellationToken = default)
    {
        if (userIds.Count == 0)
        {
            return [];
        }

        var parameters = userIds
            .Distinct(StringComparer.Ordinal)
            .Select((value, index) => ($"$user_id_{index}", (object?)value))
            .ToArray();

        var placeholders = string.Join(", ", parameters.Select(parameter => parameter.Item1));
        var items = new List<PoolUser>();

        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, $"""
            SELECT
                user_id,
                username,
                password_hash_hex,
                password_salt_hex,
                deposit_address_hex,
                protected_deposit_private_key_hex,
                withdrawal_address_hex,
                last_observed_deposit_atomic,
                last_observed_deposit_height_text,
                created_utc
            FROM users
            WHERE user_id IN ({placeholders});
            """,
            parameters);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            items.Add(MapUser(reader));
        }

        return items;
    }

    public async Task<List<PoolUser>> ListUsersAsync(CancellationToken cancellationToken = default)
    {
        var items = new List<PoolUser>();
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            SELECT
                user_id,
                username,
                password_hash_hex,
                password_salt_hex,
                deposit_address_hex,
                protected_deposit_private_key_hex,
                withdrawal_address_hex,
                last_observed_deposit_atomic,
                last_observed_deposit_height_text,
                created_utc
            FROM users
            ORDER BY username;
            """);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            items.Add(MapUser(reader));
        }

        return items;
    }

    public async Task<List<UserBalanceView>> ListUsersWithBalancesAsync(CancellationToken cancellationToken = default)
    {
        var items = new List<UserBalanceView>();
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            SELECT
                u.user_id,
                u.username,
                u.deposit_address_hex,
                u.withdrawal_address_hex,
                b.available_atomic,
                b.pending_withdrawal_atomic,
                b.total_mined_atomic,
                b.total_deposited_atomic,
                b.total_withdrawn_atomic,
                COALESCE((
                    SELECT SUM(fbp.amount_atomic)
                    FROM found_block_payouts fbp
                    WHERE fbp.user_id = u.user_id
                      AND fbp.status = $pending_status
                ), 0)
            FROM users u
            INNER JOIN balances b ON b.user_id = u.user_id
            ORDER BY u.username;
            """,
            ("$pending_status", (int)FoundBlockPayoutStatus.Pending));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            items.Add(new UserBalanceView(
                reader.GetString(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.GetInt64(4),
                reader.GetInt64(5),
                reader.GetInt64(6),
                reader.GetInt64(7),
                reader.GetInt64(8),
                reader.GetInt64(9)));
        }

        return items;
    }

    public async Task UpdateUserWithdrawalAddressAsync(string userId, string? withdrawalAddressHex, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await UpdateUserWithdrawalAddressAsync(userId, withdrawalAddressHex, connection, null, cancellationToken).ConfigureAwait(false);
    }

    internal async Task UpdateUserWithdrawalAddressAsync(string userId, string? withdrawalAddressHex, SqliteConnection connection, SqliteTransaction? transaction, CancellationToken cancellationToken = default)
    {
        await using var command = CreateCommand(connection, transaction, """
            UPDATE users
            SET withdrawal_address_hex = $withdrawal_address_hex
            WHERE user_id = $user_id;
            """,
            ("$user_id", userId),
            ("$withdrawal_address_hex", withdrawalAddressHex));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    internal async Task UpdateUserDepositObservationAsync(string userId, long observedAtomic, string heightText, SqliteConnection connection, SqliteTransaction transaction, CancellationToken cancellationToken = default)
    {
        await using var command = CreateCommand(connection, transaction, """
            UPDATE users
            SET last_observed_deposit_atomic = $last_observed_deposit_atomic,
                last_observed_deposit_height_text = $last_observed_deposit_height_text
            WHERE user_id = $user_id;
            """,
            ("$user_id", userId),
            ("$last_observed_deposit_atomic", observedAtomic),
            ("$last_observed_deposit_height_text", heightText));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task InsertSessionAsync(UserSession session, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            INSERT INTO sessions (
                session_id,
                user_id,
                token_hash_hex,
                created_utc,
                expires_utc,
                last_seen_utc
            )
            VALUES (
                $session_id,
                $user_id,
                $token_hash_hex,
                $created_utc,
                $expires_utc,
                $last_seen_utc
            );
            """,
            ("$session_id", session.SessionId),
            ("$user_id", session.UserId),
            ("$token_hash_hex", session.TokenHashHex),
            ("$created_utc", ToDb(session.CreatedUtc)),
            ("$expires_utc", ToDb(session.ExpiresUtc)),
            ("$last_seen_utc", ToDb(session.LastSeenUtc)));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<UserSession?> GetSessionByTokenHashAsync(string tokenHashHex, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            SELECT
                session_id,
                user_id,
                token_hash_hex,
                created_utc,
                expires_utc,
                last_seen_utc
            FROM sessions
            WHERE token_hash_hex = $token_hash_hex
            LIMIT 1;
            """,
            ("$token_hash_hex", tokenHashHex));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false) ? MapSession(reader) : null;
    }

    public async Task TouchSessionAsync(string sessionId, DateTimeOffset lastSeenUtc, DateTimeOffset expiresUtc, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            UPDATE sessions
            SET last_seen_utc = $last_seen_utc,
                expires_utc = $expires_utc
            WHERE session_id = $session_id;
            """,
            ("$session_id", sessionId),
            ("$last_seen_utc", ToDb(lastSeenUtc)),
            ("$expires_utc", ToDb(expiresUtc)));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task DeleteSessionAsync(string sessionId, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, "DELETE FROM sessions WHERE session_id = $session_id;", ("$session_id", sessionId));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<MinerRecord?> GetMinerByUserIdAsync(string userId, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        return await GetMinerByUserIdAsync(userId, connection, null, cancellationToken).ConfigureAwait(false);
    }

    internal async Task<MinerRecord?> GetMinerByUserIdAsync(string userId, SqliteConnection connection, SqliteTransaction? transaction, CancellationToken cancellationToken = default)
    {
        await using var command = CreateCommand(connection, transaction, """
            SELECT
                miner_id,
                user_id,
                public_key_hex,
                share_difficulty,
                api_token_hash_hex,
                api_token_text,
                is_verified,
                verified_utc,
                last_job_utc,
                last_share_utc
            FROM miners
            WHERE user_id = $user_id
            LIMIT 1;
            """,
            ("$user_id", userId));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false) ? MapMiner(reader) : null;
    }

    public async Task<MinerRecord?> GetMinerByApiTokenHashAsync(string tokenHashHex, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            SELECT
                miner_id,
                user_id,
                public_key_hex,
                share_difficulty,
                api_token_hash_hex,
                api_token_text,
                is_verified,
                verified_utc,
                last_job_utc,
                last_share_utc
            FROM miners
            WHERE api_token_hash_hex = $api_token_hash_hex
            LIMIT 1;
            """,
            ("$api_token_hash_hex", tokenHashHex));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false) ? MapMiner(reader) : null;
    }

    public async Task<MinerRecord?> GetMinerByIdAsync(string minerId, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        return await GetMinerByIdAsync(minerId, connection, null, cancellationToken).ConfigureAwait(false);
    }

    internal async Task<MinerRecord?> GetMinerByIdAsync(string minerId, SqliteConnection connection, SqliteTransaction? transaction, CancellationToken cancellationToken = default)
    {
        await using var command = CreateCommand(connection, transaction, """
            SELECT
                miner_id,
                user_id,
                public_key_hex,
                share_difficulty,
                api_token_hash_hex,
                api_token_text,
                is_verified,
                verified_utc,
                last_job_utc,
                last_share_utc
            FROM miners
            WHERE miner_id = $miner_id
            LIMIT 1;
            """,
            ("$miner_id", minerId));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false) ? MapMiner(reader) : null;
    }

    public async Task UpsertMinerAsync(MinerRecord miner, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await UpsertMinerAsync(miner, connection, null, cancellationToken).ConfigureAwait(false);
    }

    internal async Task UpsertMinerAsync(MinerRecord miner, SqliteConnection connection, SqliteTransaction? transaction, CancellationToken cancellationToken = default)
    {
        var normalizedDifficulty = DifficultyFixedPoint.ToNormalizedDouble(miner.ShareDifficulty);
        await using var command = CreateCommand(connection, transaction, """
            INSERT INTO miners (
                miner_id,
                user_id,
                public_key_hex,
                share_difficulty,
                api_token_hash_hex,
                api_token_text,
                is_verified,
                verified_utc,
                last_job_utc,
                last_share_utc
            )
            VALUES (
                $miner_id,
                $user_id,
                $public_key_hex,
                $share_difficulty,
                $api_token_hash_hex,
                $api_token_text,
                $is_verified,
                $verified_utc,
                $last_job_utc,
                $last_share_utc
            )
            ON CONFLICT(user_id) DO UPDATE SET
                public_key_hex = excluded.public_key_hex,
                share_difficulty = excluded.share_difficulty,
                api_token_hash_hex = excluded.api_token_hash_hex,
                api_token_text = excluded.api_token_text,
                is_verified = excluded.is_verified,
                verified_utc = excluded.verified_utc,
                last_job_utc = excluded.last_job_utc,
                last_share_utc = excluded.last_share_utc;
            """,
            ("$miner_id", miner.MinerId),
            ("$user_id", miner.UserId),
            ("$public_key_hex", miner.PublicKeyHex),
            ("$share_difficulty", normalizedDifficulty),
            ("$api_token_hash_hex", miner.ApiTokenHashHex),
            ("$api_token_text", miner.ApiTokenText),
            ("$is_verified", ToDbBool(miner.IsVerified)),
            ("$verified_utc", ToDb(miner.VerifiedUtc)),
            ("$last_job_utc", ToDb(miner.LastJobUtc)),
            ("$last_share_utc", miner.LastShareUtc is null ? null : ToDb(miner.LastShareUtc.Value)));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task UpdateMinerDifficultyAsync(string minerId, double shareDifficulty, CancellationToken cancellationToken = default)
    {
        var normalizedDifficulty = DifficultyFixedPoint.ToNormalizedDouble(shareDifficulty);
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            UPDATE miners
            SET share_difficulty = $share_difficulty
            WHERE miner_id = $miner_id;
            """,
            ("$miner_id", minerId),
            ("$share_difficulty", normalizedDifficulty));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task UpdateMinerHeartbeatsAsync(string minerId, DateTimeOffset? lastJobUtc, DateTimeOffset? lastShareUtc, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await UpdateMinerHeartbeatsAsync(minerId, lastJobUtc, lastShareUtc, connection, null, cancellationToken).ConfigureAwait(false);
    }

    internal async Task UpdateMinerHeartbeatsAsync(string minerId, DateTimeOffset? lastJobUtc, DateTimeOffset? lastShareUtc, SqliteConnection connection, SqliteTransaction? transaction, CancellationToken cancellationToken = default)
    {
        await using var command = CreateCommand(connection, transaction, """
            UPDATE miners
            SET last_job_utc = COALESCE($last_job_utc, last_job_utc),
                last_share_utc = COALESCE($last_share_utc, last_share_utc)
            WHERE miner_id = $miner_id;
            """,
            ("$miner_id", minerId),
            ("$last_job_utc", lastJobUtc is null ? null : ToDb(lastJobUtc.Value)),
            ("$last_share_utc", lastShareUtc is null ? null : ToDb(lastShareUtc.Value)));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<List<MinerRecord>> ListVerifiedMinersAsync(CancellationToken cancellationToken = default)
    {
        var items = new List<MinerRecord>();
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            SELECT
                miner_id,
                user_id,
                public_key_hex,
                share_difficulty,
                api_token_hash_hex,
                api_token_text,
                is_verified,
                verified_utc,
                last_job_utc,
                last_share_utc
            FROM miners
            WHERE is_verified = 1
            ORDER BY verified_utc DESC;
            """);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            items.Add(MapMiner(reader));
        }

        return items;
    }

    public async Task InsertChallengeAsync(MinerChallenge challenge, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            INSERT INTO challenges (
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

    public async Task<MinerChallenge?> GetChallengeByIdAsync(string challengeId, CancellationToken cancellationToken = default)
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
            FROM challenges
            WHERE challenge_id = $challenge_id
            LIMIT 1;
            """,
            ("$challenge_id", challengeId));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false) ? MapChallenge(reader) : null;
    }

    public async Task ConsumeChallengeAsync(string challengeId, DateTimeOffset consumedUtc, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            UPDATE challenges
            SET consumed_utc = $consumed_utc
            WHERE challenge_id = $challenge_id;
            """,
            ("$challenge_id", challengeId),
            ("$consumed_utc", ToDb(consumedUtc)));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<PoolRound?> GetOpenRoundAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            SELECT
                round_id,
                node_job_id,
                height_text,
                prev_hash_hex,
                network_target_hex,
                header_hex_zero_nonce,
                coinbase_amount_text,
                opened_utc,
                closed_utc,
                status,
                block_hash_hex
            FROM rounds
            WHERE status = $status
            ORDER BY round_id DESC
            LIMIT 1;
            """,
            ("$status", (int)RoundStatus.Open));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false) ? MapRound(reader) : null;
    }

    public async Task<PoolRound?> GetOpenRoundByPrevHashAsync(string prevHashHex, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            SELECT
                round_id,
                node_job_id,
                height_text,
                prev_hash_hex,
                network_target_hex,
                header_hex_zero_nonce,
                coinbase_amount_text,
                opened_utc,
                closed_utc,
                status,
                block_hash_hex
            FROM rounds
            WHERE prev_hash_hex = $prev_hash_hex
              AND status = $status
            ORDER BY round_id DESC
            LIMIT 1;
            """,
            ("$prev_hash_hex", prevHashHex),
            ("$status", (int)RoundStatus.Open));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false) ? MapRound(reader) : null;
    }

    public async Task<PoolRound> ResolveOpenRoundAsync(
        string nodeJobId,
        string heightText,
        string prevHashHex,
        string networkTargetHex,
        string headerHexZeroNonce,
        string coinbaseAmountText,
        CancellationToken cancellationToken = default)
    {
        await _roundStateGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return await ExecuteInTransactionAsync(async (connection, transaction) =>
            {
                var now = DateTimeOffset.UtcNow;
                var openRounds = await ListOpenRoundsAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
                var selectedRound = openRounds
                    .Where(round => string.Equals(round.PrevHashHex, prevHashHex, StringComparison.Ordinal))
                    .OrderByDescending(round => round.RoundId)
                    .FirstOrDefault();

                if (selectedRound is null)
                {
                    var roundId = await InsertRoundAsync(
                        new PoolRound(
                            0,
                            nodeJobId,
                            heightText,
                            prevHashHex,
                            networkTargetHex,
                            headerHexZeroNonce,
                            coinbaseAmountText,
                            now,
                            null,
                            RoundStatus.Open,
                            null),
                        connection,
                        transaction,
                        cancellationToken).ConfigureAwait(false);

                    selectedRound = await GetRoundByIdAsync(roundId, connection, transaction, cancellationToken).ConfigureAwait(false)
                        ?? throw new InvalidOperationException("Failed to load the round that was just created.");
                }

                await CloseOpenRoundsExceptAsync(selectedRound.RoundId, RoundStatus.Closed, now, connection, transaction, cancellationToken).ConfigureAwait(false);
                return selectedRound;
            }, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _roundStateGate.Release();
        }
    }

    public async Task<long> InsertRoundAsync(PoolRound round, CancellationToken cancellationToken = default)
    {
        return await ExecuteInTransactionAsync(
            (connection, transaction) => InsertRoundAsync(round, connection, transaction, cancellationToken),
            cancellationToken).ConfigureAwait(false);
    }

    internal async Task<long> InsertRoundAsync(PoolRound round, SqliteConnection connection, SqliteTransaction transaction, CancellationToken cancellationToken = default)
    {
        await using var command = CreateCommand(connection, transaction, """
            INSERT INTO rounds (
                node_job_id,
                height_text,
                prev_hash_hex,
                network_target_hex,
                header_hex_zero_nonce,
                coinbase_amount_text,
                opened_utc,
                closed_utc,
                status,
                block_hash_hex
            )
            VALUES (
                $node_job_id,
                $height_text,
                $prev_hash_hex,
                $network_target_hex,
                $header_hex_zero_nonce,
                $coinbase_amount_text,
                $opened_utc,
                $closed_utc,
                $status,
                $block_hash_hex
            );
            SELECT last_insert_rowid();
            """,
            ("$node_job_id", round.NodeJobId),
            ("$height_text", round.HeightText),
            ("$prev_hash_hex", round.PrevHashHex),
            ("$network_target_hex", round.NetworkTargetHex),
            ("$header_hex_zero_nonce", round.HeaderHexZeroNonce),
            ("$coinbase_amount_text", round.CoinbaseAmountText),
            ("$opened_utc", ToDb(round.OpenedUtc)),
            ("$closed_utc", round.ClosedUtc is null ? null : ToDb(round.ClosedUtc.Value)),
            ("$status", (int)round.Status),
            ("$block_hash_hex", round.BlockHashHex));
        var scalar = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return Convert.ToInt64(scalar, CultureInfo.InvariantCulture);
    }

    public async Task CloseOpenRoundsExceptAsync(long roundIdToKeep, RoundStatus closeStatus, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await CloseOpenRoundsExceptAsync(roundIdToKeep, closeStatus, DateTimeOffset.UtcNow, connection, null, cancellationToken).ConfigureAwait(false);
    }

    internal async Task CloseOpenRoundsExceptAsync(long roundIdToKeep, RoundStatus closeStatus, DateTimeOffset closedUtc, SqliteConnection connection, SqliteTransaction? transaction, CancellationToken cancellationToken = default)
    {
        await using var command = CreateCommand(connection, transaction, """
            UPDATE rounds
            SET status = $status,
                closed_utc = $closed_utc
            WHERE status = $open_status
              AND round_id <> $round_id;
            """,
            ("$status", (int)closeStatus),
            ("$closed_utc", ToDb(closedUtc)),
            ("$open_status", (int)RoundStatus.Open),
            ("$round_id", roundIdToKeep));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<int> CloseOpenRoundsNotMatchingPrevHashAsync(string prevHashHex, RoundStatus closeStatus, CancellationToken cancellationToken = default)
    {
        await _roundStateGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return await ExecuteInTransactionAsync(async (connection, transaction) =>
            {
                await using var command = CreateCommand(connection, transaction, """
                    UPDATE rounds
                    SET status = $status,
                        closed_utc = $closed_utc
                    WHERE status = $open_status
                      AND prev_hash_hex <> $prev_hash_hex;
                    """,
                    ("$status", (int)closeStatus),
                    ("$closed_utc", ToDb(DateTimeOffset.UtcNow)),
                    ("$open_status", (int)RoundStatus.Open),
                    ("$prev_hash_hex", prevHashHex));
                return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _roundStateGate.Release();
        }
    }

    public async Task UpdateRoundStateAsync(long roundId, RoundStatus status, string? blockHashHex, DateTimeOffset? closedUtc, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await UpdateRoundStateAsync(roundId, status, blockHashHex, closedUtc, connection, null, cancellationToken).ConfigureAwait(false);
    }

    internal async Task UpdateRoundStateAsync(long roundId, RoundStatus status, string? blockHashHex, DateTimeOffset? closedUtc, SqliteConnection connection, SqliteTransaction? transaction, CancellationToken cancellationToken = default)
    {
        await using var command = CreateCommand(connection, transaction, """
            UPDATE rounds
            SET status = $status,
                block_hash_hex = COALESCE($block_hash_hex, block_hash_hex),
                closed_utc = COALESCE($closed_utc, closed_utc)
            WHERE round_id = $round_id;
            """,
            ("$round_id", roundId),
            ("$status", (int)status),
            ("$block_hash_hex", blockHashHex),
            ("$closed_utc", closedUtc is null ? null : ToDb(closedUtc.Value)));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task InsertFoundBlockAsync(FoundBlockRecord block, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await InsertFoundBlockAsync(block, connection, null, cancellationToken).ConfigureAwait(false);
    }

    internal async Task InsertFoundBlockAsync(FoundBlockRecord block, SqliteConnection connection, SqliteTransaction? transaction, CancellationToken cancellationToken = default)
    {
        await using var command = CreateCommand(connection, transaction, """
            INSERT INTO found_blocks (
                block_id,
                round_id,
                block_hash_hex,
                height_text,
                reward_atomic_text,
                accepted_utc,
                status,
                confirmations_text,
                last_checked_utc,
                finalized_utc,
                orphaned_utc
            )
            VALUES (
                $block_id,
                $round_id,
                $block_hash_hex,
                $height_text,
                $reward_atomic_text,
                $accepted_utc,
                $status,
                $confirmations_text,
                $last_checked_utc,
                $finalized_utc,
                $orphaned_utc
            );
            """,
            ("$block_id", block.BlockId),
            ("$round_id", block.RoundId),
            ("$block_hash_hex", block.BlockHashHex),
            ("$height_text", block.HeightText),
            ("$reward_atomic_text", block.RewardAtomicText),
            ("$accepted_utc", ToDb(block.AcceptedUtc)),
            ("$status", (int)block.Status),
            ("$confirmations_text", block.ConfirmationsText),
            ("$last_checked_utc", block.LastCheckedUtc is null ? null : ToDb(block.LastCheckedUtc.Value)),
            ("$finalized_utc", block.FinalizedUtc is null ? null : ToDb(block.FinalizedUtc.Value)),
            ("$orphaned_utc", block.OrphanedUtc is null ? null : ToDb(block.OrphanedUtc.Value)));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task InsertJobAsync(PoolJob job, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            INSERT INTO jobs (
                pool_job_id,
                round_id,
                miner_id,
                node_job_id,
                height_text,
                prev_hash_hex,
                network_target_hex,
                share_target_hex,
                header_hex_zero_nonce,
                base_timestamp_text,
                coinbase_amount_text,
                created_utc,
                expires_utc,
                is_active
            )
            VALUES (
                $pool_job_id,
                $round_id,
                $miner_id,
                $node_job_id,
                $height_text,
                $prev_hash_hex,
                $network_target_hex,
                $share_target_hex,
                $header_hex_zero_nonce,
                $base_timestamp_text,
                $coinbase_amount_text,
                $created_utc,
                $expires_utc,
                $is_active
            );
            """,
            ("$pool_job_id", job.PoolJobId),
            ("$round_id", job.RoundId),
            ("$miner_id", job.MinerId),
            ("$node_job_id", job.NodeJobId),
            ("$height_text", job.HeightText),
            ("$prev_hash_hex", job.PrevHashHex),
            ("$network_target_hex", job.NetworkTargetHex),
            ("$share_target_hex", job.ShareTargetHex),
            ("$header_hex_zero_nonce", job.HeaderHexZeroNonce),
            ("$base_timestamp_text", job.BaseTimestampText),
            ("$coinbase_amount_text", job.CoinbaseAmountText),
            ("$created_utc", ToDb(job.CreatedUtc)),
            ("$expires_utc", ToDb(job.ExpiresUtc)),
            ("$is_active", ToDbBool(job.IsActive)));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<PoolJob?> GetJobByIdAsync(string poolJobId, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            SELECT
                pool_job_id,
                round_id,
                miner_id,
                node_job_id,
                height_text,
                prev_hash_hex,
                network_target_hex,
                share_target_hex,
                header_hex_zero_nonce,
                base_timestamp_text,
                coinbase_amount_text,
                created_utc,
                expires_utc,
                is_active
            FROM jobs
            WHERE pool_job_id = $pool_job_id
            LIMIT 1;
            """,
            ("$pool_job_id", poolJobId));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false) ? MapJob(reader) : null;
    }

    internal async Task<PoolRound?> GetRoundByIdAsync(long roundId, SqliteConnection connection, SqliteTransaction? transaction, CancellationToken cancellationToken = default)
    {
        await using var command = CreateCommand(connection, transaction, """
            SELECT
                round_id,
                node_job_id,
                height_text,
                prev_hash_hex,
                network_target_hex,
                header_hex_zero_nonce,
                coinbase_amount_text,
                opened_utc,
                closed_utc,
                status,
                block_hash_hex
            FROM rounds
            WHERE round_id = $round_id
            LIMIT 1;
            """,
            ("$round_id", roundId));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false) ? MapRound(reader) : null;
    }

    public async Task MarkJobInactiveAsync(string poolJobId, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, "UPDATE jobs SET is_active = 0 WHERE pool_job_id = $pool_job_id;", ("$pool_job_id", poolJobId));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<int> MarkActiveJobsInactiveForMinerAsync(string minerId, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            UPDATE jobs
            SET is_active = 0
            WHERE miner_id = $miner_id
              AND is_active = 1;
            """,
            ("$miner_id", minerId));
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<int> DeactivateExpiredJobsAsync(DateTimeOffset utcNow, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            UPDATE jobs
            SET is_active = 0
            WHERE is_active = 1
              AND expires_utc < $utc_now;
            """,
            ("$utc_now", ToDb(utcNow)));
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    internal async Task<List<PoolRound>> ListOpenRoundsAsync(SqliteConnection connection, SqliteTransaction? transaction, CancellationToken cancellationToken = default)
    {
        var items = new List<PoolRound>();
        await using var command = CreateCommand(connection, transaction, """
            SELECT
                round_id,
                node_job_id,
                height_text,
                prev_hash_hex,
                network_target_hex,
                header_hex_zero_nonce,
                coinbase_amount_text,
                opened_utc,
                closed_utc,
                status,
                block_hash_hex
            FROM rounds
            WHERE status = $status
            ORDER BY round_id DESC;
            """,
            ("$status", (int)RoundStatus.Open));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            items.Add(MapRound(reader));
        }

        return items;
    }

    public async Task<bool> InsertShareAsync(ShareRecord share, CancellationToken cancellationToken = default)
    {
        try
        {
            var normalizedDifficulty = DifficultyFixedPoint.ToNormalizedDouble(share.Difficulty);
            var difficultyScaled = DifficultyFixedPoint.ToScaled(normalizedDifficulty);
            await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using var command = CreateCommand(connection, null, """
                INSERT INTO shares (
                    round_id,
                    pool_job_id,
                    miner_id,
                    nonce_text,
                    timestamp_text,
                    hash_hex,
                    difficulty,
                    difficulty_scaled,
                    status,
                    meets_block_target,
                    submitted_utc
                )
                VALUES (
                    $round_id,
                    $pool_job_id,
                    $miner_id,
                    $nonce_text,
                    $timestamp_text,
                    $hash_hex,
                    $difficulty,
                    $difficulty_scaled,
                    $status,
                    $meets_block_target,
                    $submitted_utc
                );
                """,
                ("$round_id", share.RoundId),
                ("$pool_job_id", share.PoolJobId),
                ("$miner_id", share.MinerId),
                ("$nonce_text", share.NonceText),
                ("$timestamp_text", share.TimestampText),
                ("$hash_hex", share.HashHex),
                ("$difficulty", normalizedDifficulty),
                ("$difficulty_scaled", difficultyScaled),
                ("$status", (int)share.Status),
                ("$meets_block_target", ToDbBool(share.MeetsBlockTarget)),
                ("$submitted_utc", ToDb(share.SubmittedUtc)));
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (SqliteException ex) when (ex.SqliteErrorCode == 19)
        {
            return false;
        }
    }

    public async Task<(int accepted, int stale, int invalid)> GetMinerRoundShareSummaryAsync(string minerId, long roundId, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            SELECT status, COUNT(*)
            FROM shares
            WHERE miner_id = $miner_id
              AND round_id = $round_id
            GROUP BY status;
            """,
            ("$miner_id", minerId),
            ("$round_id", roundId));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

        var accepted = 0;
        var stale = 0;
        var invalid = 0;

        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var status = (ShareStatus)reader.GetInt32(0);
            var count = reader.GetInt32(1);
            switch (status)
            {
                case ShareStatus.Accepted:
                case ShareStatus.BlockCandidate:
                    accepted += count;
                    break;
                case ShareStatus.Stale:
                    stale += count;
                    break;
                default:
                    invalid += count;
                    break;
            }
        }

        return (accepted, stale, invalid);
    }

    public async Task<List<ShareRecord>> GetAcceptedSharesSinceAsync(string minerId, DateTimeOffset sinceUtc, CancellationToken cancellationToken = default)
    {
        var items = new List<ShareRecord>();
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            SELECT
                share_id,
                round_id,
                pool_job_id,
                miner_id,
                nonce_text,
                timestamp_text,
                hash_hex,
                difficulty,
                status,
                meets_block_target,
                submitted_utc
            FROM shares
            WHERE miner_id = $miner_id
              AND submitted_utc >= $submitted_utc
              AND status IN ($accepted_status, $block_candidate_status)
            ORDER BY submitted_utc DESC;
            """,
            ("$miner_id", minerId),
            ("$submitted_utc", ToDb(sinceUtc)),
            ("$accepted_status", (int)ShareStatus.Accepted),
            ("$block_candidate_status", (int)ShareStatus.BlockCandidate));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            items.Add(MapShare(reader));
        }

        return items;
    }

    public async Task<List<ShareRecord>> GetAcceptedSharesSinceAsync(DateTimeOffset sinceUtc, CancellationToken cancellationToken = default)
    {
        var items = new List<ShareRecord>();
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            SELECT
                share_id,
                round_id,
                pool_job_id,
                miner_id,
                nonce_text,
                timestamp_text,
                hash_hex,
                difficulty,
                status,
                meets_block_target,
                submitted_utc
            FROM shares
            WHERE submitted_utc >= $submitted_utc
              AND status IN ($accepted_status, $block_candidate_status)
            ORDER BY submitted_utc DESC;
            """,
            ("$submitted_utc", ToDb(sinceUtc)),
            ("$accepted_status", (int)ShareStatus.Accepted),
            ("$block_candidate_status", (int)ShareStatus.BlockCandidate));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            items.Add(MapShare(reader));
        }

        return items;
    }

    public async Task<List<(string minerId, long? difficultyScaled, double difficulty, long shareCount)>> GetRoundContributionWeightBucketsAsync(long roundId, CancellationToken cancellationToken = default)
    {
        var items = new List<(string minerId, long? difficultyScaled, double difficulty, long shareCount)>();
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        return await GetRoundContributionWeightBucketsAsync(roundId, connection, null, cancellationToken).ConfigureAwait(false);
    }

    internal async Task<List<(string minerId, long? difficultyScaled, double difficulty, long shareCount)>> GetRoundContributionWeightBucketsAsync(long roundId, SqliteConnection connection, SqliteTransaction? transaction, CancellationToken cancellationToken = default)
    {
        var items = new List<(string minerId, long? difficultyScaled, double difficulty, long shareCount)>();
        await using var command = CreateCommand(connection, transaction, """
            SELECT
                miner_id,
                difficulty_scaled,
                difficulty,
                COUNT(*)
            FROM shares
            WHERE round_id = $round_id
              AND status IN ($accepted_status, $block_candidate_status)
            GROUP BY miner_id, difficulty_scaled, difficulty
            ORDER BY miner_id ASC, difficulty_scaled DESC;
            """,
            ("$round_id", roundId),
            ("$accepted_status", (int)ShareStatus.Accepted),
            ("$block_candidate_status", (int)ShareStatus.BlockCandidate));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            items.Add((
                reader.GetString(0),
                reader.IsDBNull(1) ? null : reader.GetInt64(1),
                reader.GetDouble(2),
                reader.GetInt64(3)));
        }

        return items;
    }

    public async Task<List<PoolRound>> ListRecentRoundsAsync(int limit, CancellationToken cancellationToken = default)
    {
        var items = new List<PoolRound>();
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            SELECT
                round_id,
                node_job_id,
                height_text,
                prev_hash_hex,
                network_target_hex,
                header_hex_zero_nonce,
                coinbase_amount_text,
                opened_utc,
                closed_utc,
                status,
                block_hash_hex
            FROM rounds
            ORDER BY round_id DESC
            LIMIT $limit;
            """,
            ("$limit", limit));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            items.Add(MapRound(reader));
        }

        return items;
    }

    public async Task<List<FoundBlockRecord>> ListRecentBlocksAsync(int limit, CancellationToken cancellationToken = default)
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
            ORDER BY accepted_utc DESC
            LIMIT $limit;
            """,
            ("$limit", limit));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            items.Add(MapFoundBlock(reader));
        }

        return items;
    }

    public async Task EnsureBalanceAsync(string userId, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await EnsureBalanceAsync(userId, connection, null, cancellationToken).ConfigureAwait(false);
    }

    internal async Task EnsureBalanceAsync(string userId, SqliteConnection connection, SqliteTransaction? transaction, CancellationToken cancellationToken = default)
    {
        await using var command = CreateCommand(connection, transaction, """
            INSERT OR IGNORE INTO balances (
                user_id,
                available_atomic,
                pending_withdrawal_atomic,
                total_mined_atomic,
                total_deposited_atomic,
                total_withdrawn_atomic,
                updated_utc
            )
            VALUES (
                $user_id,
                0,
                0,
                0,
                0,
                0,
                $updated_utc
            );
            """,
            ("$user_id", userId),
            ("$updated_utc", ToDb(DateTimeOffset.UtcNow)));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<BalanceRecord?> GetBalanceAsync(string userId, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        return await GetBalanceAsync(userId, connection, null, cancellationToken).ConfigureAwait(false);
    }

    internal async Task<BalanceRecord?> GetBalanceAsync(string userId, SqliteConnection connection, SqliteTransaction? transaction, CancellationToken cancellationToken = default)
    {
        await using var command = CreateCommand(connection, transaction, """
            SELECT
                user_id,
                available_atomic,
                pending_withdrawal_atomic,
                total_mined_atomic,
                total_deposited_atomic,
                total_withdrawn_atomic,
                updated_utc
            FROM balances
            WHERE user_id = $user_id
            LIMIT 1;
            """,
            ("$user_id", userId));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false) ? MapBalance(reader) : null;
    }

    internal async Task UpdateBalanceAsync(
        string userId,
        long availableDelta,
        long pendingWithdrawalDelta,
        long totalMinedDelta,
        long totalDepositedDelta,
        long totalWithdrawnDelta,
        SqliteConnection connection,
        SqliteTransaction transaction,
        CancellationToken cancellationToken = default)
    {
        await EnsureBalanceAsync(userId, connection, transaction, cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, transaction, """
            UPDATE balances
            SET available_atomic = available_atomic + $available_delta,
                pending_withdrawal_atomic = pending_withdrawal_atomic + $pending_withdrawal_delta,
                total_mined_atomic = total_mined_atomic + $total_mined_delta,
                total_deposited_atomic = total_deposited_atomic + $total_deposited_delta,
                total_withdrawn_atomic = total_withdrawn_atomic + $total_withdrawn_delta,
                updated_utc = $updated_utc
            WHERE user_id = $user_id;
            """,
            ("$user_id", userId),
            ("$available_delta", availableDelta),
            ("$pending_withdrawal_delta", pendingWithdrawalDelta),
            ("$total_mined_delta", totalMinedDelta),
            ("$total_deposited_delta", totalDepositedDelta),
            ("$total_withdrawn_delta", totalWithdrawnDelta),
            ("$updated_utc", ToDb(DateTimeOffset.UtcNow)));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task InsertLedgerEntryAsync(LedgerEntryRecord entry, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await InsertLedgerEntryAsync(entry, connection, null, cancellationToken).ConfigureAwait(false);
    }

    internal async Task InsertLedgerEntryAsync(LedgerEntryRecord entry, SqliteConnection connection, SqliteTransaction? transaction, CancellationToken cancellationToken = default)
    {
        await using var command = CreateCommand(connection, transaction, """
            INSERT INTO ledger_entries (
                ledger_entry_id,
                user_id,
                entry_type,
                delta_atomic,
                reference,
                metadata_json,
                created_utc
            )
            VALUES (
                $ledger_entry_id,
                $user_id,
                $entry_type,
                $delta_atomic,
                $reference,
                $metadata_json,
                $created_utc
            );
            """,
            ("$ledger_entry_id", entry.LedgerEntryId),
            ("$user_id", entry.UserId),
            ("$entry_type", (int)entry.EntryType),
            ("$delta_atomic", entry.DeltaAtomic),
            ("$reference", entry.Reference),
            ("$metadata_json", entry.MetadataJson),
            ("$created_utc", ToDb(entry.CreatedUtc)));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<List<LedgerEntryRecord>> ListLedgerEntriesByUserIdAsync(string userId, CancellationToken cancellationToken = default)
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
            ORDER BY created_utc DESC, ledger_entry_id DESC;
            """,
            ("$user_id", userId));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            items.Add(MapLedgerEntry(reader));
        }

        return items;
    }

    public async Task InsertWithdrawalRequestAsync(WithdrawalRequestRecord request, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await InsertWithdrawalRequestAsync(request, connection, null, cancellationToken).ConfigureAwait(false);
    }

    internal async Task InsertWithdrawalRequestAsync(WithdrawalRequestRecord request, SqliteConnection connection, SqliteTransaction? transaction, CancellationToken cancellationToken = default)
    {
        await using var command = CreateCommand(connection, transaction, """
            INSERT INTO withdrawal_requests (
                withdrawal_id,
                user_id,
                amount_atomic,
                destination_address_hex,
                status,
                admin_note,
                external_txid,
                requested_utc,
                updated_utc
            )
            VALUES (
                $withdrawal_id,
                $user_id,
                $amount_atomic,
                $destination_address_hex,
                $status,
                $admin_note,
                $external_txid,
                $requested_utc,
                $updated_utc
            );
            """,
            ("$withdrawal_id", request.WithdrawalId),
            ("$user_id", request.UserId),
            ("$amount_atomic", request.AmountAtomic),
            ("$destination_address_hex", request.DestinationAddressHex),
            ("$status", (int)request.Status),
            ("$admin_note", request.AdminNote),
            ("$external_txid", request.ExternalTxId),
            ("$requested_utc", ToDb(request.RequestedUtc)),
            ("$updated_utc", ToDb(request.UpdatedUtc)));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<WithdrawalRequestRecord?> GetWithdrawalByIdAsync(string withdrawalId, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        return await GetWithdrawalByIdAsync(withdrawalId, connection, null, cancellationToken).ConfigureAwait(false);
    }

    internal async Task<WithdrawalRequestRecord?> GetWithdrawalByIdAsync(string withdrawalId, SqliteConnection connection, SqliteTransaction? transaction, CancellationToken cancellationToken = default)
    {
        await using var command = CreateCommand(connection, transaction, """
            SELECT
                withdrawal_id,
                user_id,
                amount_atomic,
                destination_address_hex,
                status,
                admin_note,
                external_txid,
                requested_utc,
                updated_utc
            FROM withdrawal_requests
            WHERE withdrawal_id = $withdrawal_id
            LIMIT 1;
            """,
            ("$withdrawal_id", withdrawalId));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false) ? MapWithdrawal(reader) : null;
    }

    public async Task<List<WithdrawalRequestRecord>> ListWithdrawalRequestsAsync(WithdrawalStatus? status = null, CancellationToken cancellationToken = default)
    {
        var items = new List<WithdrawalRequestRecord>();
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        var sql = """
            SELECT
                withdrawal_id,
                user_id,
                amount_atomic,
                destination_address_hex,
                status,
                admin_note,
                external_txid,
                requested_utc,
                updated_utc
            FROM withdrawal_requests
            """;

        if (status is not null)
        {
            sql += " WHERE status = $status";
        }

        sql += " ORDER BY requested_utc DESC;";
        await using var command = status is null
            ? CreateCommand(connection, null, sql)
            : CreateCommand(connection, null, sql, ("$status", (int)status.Value));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            items.Add(MapWithdrawal(reader));
        }

        return items;
    }

    public async Task UpdateWithdrawalRequestAsync(string withdrawalId, WithdrawalStatus status, string? adminNote, string? externalTxId, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await UpdateWithdrawalRequestAsync(withdrawalId, status, adminNote, externalTxId, connection, null, cancellationToken).ConfigureAwait(false);
    }

    internal async Task UpdateWithdrawalRequestAsync(string withdrawalId, WithdrawalStatus status, string? adminNote, string? externalTxId, SqliteConnection connection, SqliteTransaction? transaction, CancellationToken cancellationToken = default)
    {
        await using var command = CreateCommand(connection, transaction, """
            UPDATE withdrawal_requests
            SET status = $status,
                admin_note = $admin_note,
                external_txid = $external_txid,
                updated_utc = $updated_utc
            WHERE withdrawal_id = $withdrawal_id;
            """,
            ("$withdrawal_id", withdrawalId),
            ("$status", (int)status),
            ("$admin_note", adminNote),
            ("$external_txid", externalTxId),
            ("$updated_utc", ToDb(DateTimeOffset.UtcNow)));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<long> GetTotalTrackedBalanceAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, "SELECT COALESCE(SUM(available_atomic + pending_withdrawal_atomic), 0) FROM balances;");
        var scalar = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return Convert.ToInt64(scalar, CultureInfo.InvariantCulture);
    }

    public async Task<long> GetTotalImmatureMiningObligationAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(
            connection,
            null,
            "SELECT COALESCE(SUM(amount_atomic), 0) FROM found_block_payouts WHERE status = $status;",
            ("$status", (int)FoundBlockPayoutStatus.Pending));
        var scalar = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return Convert.ToInt64(scalar, CultureInfo.InvariantCulture);
    }

    public async Task<long> GetUserImmatureMiningAtomicAsync(string userId, CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(
            connection,
            null,
            """
            SELECT COALESCE(SUM(amount_atomic), 0)
            FROM found_block_payouts
            WHERE user_id = $user_id
              AND status = $status;
            """,
            ("$user_id", userId),
            ("$status", (int)FoundBlockPayoutStatus.Pending));
        var scalar = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return Convert.ToInt64(scalar, CultureInfo.InvariantCulture);
    }

    public async Task<int> CountUsersAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, "SELECT COUNT(*) FROM users;");
        var scalar = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return Convert.ToInt32(scalar, CultureInfo.InvariantCulture);
    }

    public async Task<int> CountVerifiedMinersAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, "SELECT COUNT(*) FROM miners WHERE is_verified = 1;");
        var scalar = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return Convert.ToInt32(scalar, CultureInfo.InvariantCulture);
    }

    public async Task<int> CountPendingWithdrawalsAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, """
            SELECT COUNT(*)
            FROM withdrawal_requests
            WHERE status IN ($queued, $awaiting_admin);
            """,
            ("$queued", (int)WithdrawalStatus.Queued),
            ("$awaiting_admin", (int)WithdrawalStatus.AwaitingAdmin));
        var scalar = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return Convert.ToInt32(scalar, CultureInfo.InvariantCulture);
    }

    public async Task<int> CountOpenRoundSharesAsync(CancellationToken cancellationToken = default)
    {
        var openRound = await GetOpenRoundAsync(cancellationToken).ConfigureAwait(false);
        if (openRound is null)
        {
            return 0;
        }

        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = CreateCommand(connection, null, "SELECT COUNT(*) FROM shares WHERE round_id = $round_id;", ("$round_id", openRound.RoundId));
        var scalar = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return Convert.ToInt32(scalar, CultureInfo.InvariantCulture);
    }

    private static PoolUser MapUser(SqliteDataReader reader)
    {
        return new PoolUser(
            reader.GetString(0),
            reader.GetString(1),
            reader.GetString(2),
            reader.GetString(3),
            reader.GetString(4),
            reader.GetString(5),
            reader.IsDBNull(6) ? null : reader.GetString(6),
            reader.GetInt64(7),
            reader.GetString(8),
            ParseDbTime(reader.GetString(9)));
    }

    private static UserSession MapSession(SqliteDataReader reader)
    {
        return new UserSession(
            reader.GetString(0),
            reader.GetString(1),
            reader.GetString(2),
            ParseDbTime(reader.GetString(3)),
            ParseDbTime(reader.GetString(4)),
            ParseDbTime(reader.GetString(5)));
    }

    private static MinerRecord MapMiner(SqliteDataReader reader)
    {
        return new MinerRecord(
            reader.GetString(0),
            reader.GetString(1),
            reader.GetString(2),
            DifficultyFixedPoint.ToNormalizedDouble(reader.GetDouble(3)),
            reader.GetString(4),
            reader.IsDBNull(5) ? null : reader.GetString(5),
            reader.GetInt32(6) == 1,
            ParseDbTime(reader.GetString(7)),
            ParseDbTime(reader.GetString(8)),
            reader.IsDBNull(9) ? null : ParseDbTime(reader.GetString(9)));
    }

    private static MinerChallenge MapChallenge(SqliteDataReader reader)
    {
        return new MinerChallenge(
            reader.GetString(0),
            reader.GetString(1),
            reader.GetString(2),
            reader.GetString(3),
            ParseDbTime(reader.GetString(4)),
            ParseDbTime(reader.GetString(5)),
            reader.IsDBNull(6) ? null : ParseDbTime(reader.GetString(6)));
    }

    private static PoolRound MapRound(SqliteDataReader reader)
    {
        return new PoolRound(
            reader.GetInt64(0),
            reader.GetString(1),
            reader.GetString(2),
            reader.GetString(3),
            reader.GetString(4),
            reader.GetString(5),
            reader.GetString(6),
            ParseDbTime(reader.GetString(7)),
            reader.IsDBNull(8) ? null : ParseDbTime(reader.GetString(8)),
            (RoundStatus)reader.GetInt32(9),
            reader.IsDBNull(10) ? null : reader.GetString(10));
    }

    private static PoolJob MapJob(SqliteDataReader reader)
    {
        return new PoolJob(
            reader.GetString(0),
            reader.GetInt64(1),
            reader.GetString(2),
            reader.GetString(3),
            reader.GetString(4),
            reader.GetString(5),
            reader.GetString(6),
            reader.GetString(7),
            reader.GetString(8),
            reader.GetString(9),
            reader.GetString(10),
            ParseDbTime(reader.GetString(11)),
            ParseDbTime(reader.GetString(12)),
            reader.GetInt32(13) == 1);
    }

    private static ShareRecord MapShare(SqliteDataReader reader)
    {
        return new ShareRecord(
            reader.GetInt64(0),
            reader.GetInt64(1),
            reader.GetString(2),
            reader.GetString(3),
            reader.GetString(4),
            reader.GetString(5),
            reader.GetString(6),
            DifficultyFixedPoint.ToNormalizedDouble(reader.GetDouble(7)),
            (ShareStatus)reader.GetInt32(8),
            reader.GetInt32(9) == 1,
            ParseDbTime(reader.GetString(10)));
    }

    private static FoundBlockRecord MapFoundBlock(SqliteDataReader reader)
    {
        return new FoundBlockRecord(
            reader.GetString(0),
            reader.GetInt64(1),
            reader.GetString(2),
            reader.GetString(3),
            reader.GetString(4),
            (FoundBlockStatus)reader.GetInt32(5),
            reader.GetString(6),
            ParseDbTime(reader.GetString(7)),
            reader.IsDBNull(8) ? null : ParseDbTime(reader.GetString(8)),
            reader.IsDBNull(9) ? null : ParseDbTime(reader.GetString(9)),
            reader.IsDBNull(10) ? null : ParseDbTime(reader.GetString(10)));
    }

    private static BalanceRecord MapBalance(SqliteDataReader reader)
    {
        return new BalanceRecord(
            reader.GetString(0),
            reader.GetInt64(1),
            reader.GetInt64(2),
            reader.GetInt64(3),
            reader.GetInt64(4),
            reader.GetInt64(5),
            ParseDbTime(reader.GetString(6)));
    }

    private static LedgerEntryRecord MapLedgerEntry(SqliteDataReader reader)
    {
        return new LedgerEntryRecord(
            reader.GetString(0),
            reader.GetString(1),
            (LedgerEntryType)reader.GetInt32(2),
            reader.GetInt64(3),
            reader.GetString(4),
            reader.GetString(5),
            ParseDbTime(reader.GetString(6)));
    }

    private static WithdrawalRequestRecord MapWithdrawal(SqliteDataReader reader)
    {
        return new WithdrawalRequestRecord(
            reader.GetString(0),
            reader.GetString(1),
            reader.GetInt64(2),
            reader.GetString(3),
            (WithdrawalStatus)reader.GetInt32(4),
            reader.IsDBNull(5) ? null : reader.GetString(5),
            reader.IsDBNull(6) ? null : reader.GetString(6),
            ParseDbTime(reader.GetString(7)),
            ParseDbTime(reader.GetString(8)));
    }

    private static SqliteCommand CreateCommand(SqliteConnection connection, SqliteTransaction? transaction, string sql, params (string name, object? value)[] parameters)
    {
        var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = sql;
        foreach (var (name, value) in parameters)
        {
            command.Parameters.AddWithValue(name, value ?? DBNull.Value);
        }

        return command;
    }

    private static string ToDb(DateTimeOffset value) => value.UtcDateTime.ToString("O", CultureInfo.InvariantCulture);

    private static DateTimeOffset ParseDbTime(string value) => DateTimeOffset.Parse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);

    private static int ToDbBool(bool value) => value ? 1 : 0;
}
