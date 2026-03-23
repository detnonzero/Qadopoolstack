namespace QadoPoolStack.Desktop.Api.Models;

public sealed record RegisterRequest(string Username, string Password);

public sealed record PublicPoolConfigResponse(bool AccountRegistrationEnabled);

public sealed record LoginRequest(string Username, string Password);

public sealed record AuthResponse(string Username, string SessionToken, string DepositAddress);

public sealed record ChallengeRequest(string PublicKey);

public sealed record VerifyChallengeRequest(string ChallengeId, string Signature);

public sealed record MinerAuthResponse(string PublicKey, string ApiToken, double ShareDifficulty);

public sealed record MiningShareSubmitRequest(string JobId, string Nonce, string? Timestamp);

public sealed record WithdrawRequest(string Amount, string Fee);

public sealed record DepositRequest();

public sealed record TransferRequest(string Username, string Amount, string? Note);

public sealed record PoolJobResponse(
    string JobId,
    string Height,
    string PrevHash,
    string NetworkTarget,
    string ShareTarget,
    string Timestamp,
    string MerkleRoot,
    string CoinbaseAmount,
    int TxCount,
    string HeaderHexZeroNonce,
    string PrecomputedCv,
    string Block1Base,
    string Block2,
    string[] TargetWords);

public sealed record BalanceDto(
    string Available,
    string PendingWithdrawal,
    string TotalMined,
    string TotalDeposited,
    string TotalWithdrawn);

public sealed record MeResponse(
    string Username,
    string DepositAddress,
    string? WithdrawalAddress,
    BalanceDto Balance,
    string? MinerPublicKey,
    double? MinerDifficulty,
    string? MinerApiToken);

public sealed record WithdrawResponse(string WithdrawalId, string Status, string? TxId, string SentAmount, string Fee);

public sealed record DepositResponse(string DepositAddress, BalanceDto Balance);

public sealed record TransferResponse(string Status);

public sealed record LedgerHistoryItemResponse(
    string LedgerEntryId,
    string Kind,
    string CounterpartyLabel,
    string Counterparty,
    string Amount,
    bool IsOutgoing,
    string? Note,
    string? TxId,
    DateTimeOffset CreatedUtc);

public sealed record ShareSubmitResponse(
    bool Accepted,
    bool Duplicate,
    bool Stale,
    bool BlockCandidate,
    bool BlockAccepted,
    string? Hash,
    string? Reason,
    bool ReloadJob,
    double ShareDifficulty);

public sealed record MinerStatsResponse(
    string PublicKey,
    string Username,
    double ShareDifficulty,
    int AcceptedSharesRound,
    int StaleSharesRound,
    int InvalidSharesRound,
    string RoundId,
    string EstimatedHashrate,
    DateTimeOffset? LastShareUtc);
