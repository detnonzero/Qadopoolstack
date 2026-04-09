namespace QadoPoolStack.Desktop.Domain;

public enum ShareStatus
{
    Accepted = 1,
    Duplicate = 2,
    Stale = 3,
    Invalid = 4,
    BlockCandidate = 5
}

public enum RoundStatus
{
    Open = 1,
    Won = 2,
    Closed = 3,
    Orphaned = 4
}

public enum FoundBlockStatus
{
    Pending = 1,
    Finalized = 2,
    Orphaned = 3,
    Reversed = 4
}

public enum FoundBlockPayoutStatus
{
    Pending = 1,
    Finalized = 2,
    Orphaned = 3,
    Reversed = 4
}

public enum LedgerEntryType
{
    DepositCredit = 1,
    BlockReward = 2,
    InternalTransferOut = 3,
    InternalTransferIn = 4,
    WithdrawalReserve = 5,
    WithdrawalRelease = 6,
    ManualAdjustment = 7
}

public enum WithdrawalStatus
{
    Queued = 1,
    AwaitingAdmin = 2,
    Paid = 3,
    Rejected = 4
}

public sealed record PoolUser(
    string UserId,
    string Username,
    string PasswordHashHex,
    string PasswordSaltHex,
    string DepositAddressHex,
    string ProtectedDepositPrivateKeyHex,
    string? WithdrawalAddressHex,
    long LastObservedDepositAtomic,
    string LastObservedDepositHeightText,
    DateTimeOffset CreatedUtc,
    string? CustodianPublicKeyHex = null,
    string? ProtectedCustodianPrivateKeyHex = null);

public sealed record UserSession(
    string SessionId,
    string UserId,
    string TokenHashHex,
    DateTimeOffset CreatedUtc,
    DateTimeOffset ExpiresUtc,
    DateTimeOffset LastSeenUtc);

public sealed record MinerRecord(
    string MinerId,
    string UserId,
    string PublicKeyHex,
    double ShareDifficulty,
    string ApiTokenHashHex,
    string? ApiTokenText,
    bool IsVerified,
    DateTimeOffset VerifiedUtc,
    DateTimeOffset LastJobUtc,
    DateTimeOffset? LastShareUtc);

public sealed record DepositSenderChallenge(
    string ChallengeId,
    string UserId,
    string PublicKeyHex,
    string Message,
    DateTimeOffset CreatedUtc,
    DateTimeOffset ExpiresUtc,
    DateTimeOffset? ConsumedUtc);

public sealed record VerifiedDepositSender(
    string SenderId,
    string UserId,
    string PublicKeyHex,
    DateTimeOffset VerifiedUtc);

public sealed record IncomingDepositEvent(
    string EventId,
    string TxId,
    string Status,
    string BlockHeightText,
    string BlockHashHex,
    string ConfirmationsText,
    DateTimeOffset TimestampUtc,
    string ToAddressHex,
    long AmountAtomic,
    string? FromAddressHex,
    string FromAddressesJson,
    int TxIndex,
    int TransferIndex,
    DateTimeOffset ObservedUtc,
    string? CreditedUserId,
    string? MatchedSenderPublicKeyHex,
    DateTimeOffset? CreditedUtc,
    DateTimeOffset? IgnoredUtc,
    string? IgnoreReason);

public sealed record DepositSyncState(
    string StreamKey,
    string AddressHex,
    string NextCursor,
    string TipHeightText,
    DateTimeOffset UpdatedUtc);

public sealed record PoolRound(
    long RoundId,
    string NodeJobId,
    string HeightText,
    string PrevHashHex,
    string NetworkTargetHex,
    string HeaderHexZeroNonce,
    string CoinbaseAmountText,
    DateTimeOffset OpenedUtc,
    DateTimeOffset? ClosedUtc,
    RoundStatus Status,
    string? BlockHashHex);

public sealed record PoolJob(
    string PoolJobId,
    long RoundId,
    string MinerId,
    string NodeJobId,
    string HeightText,
    string PrevHashHex,
    string NetworkTargetHex,
    string ShareTargetHex,
    string HeaderHexZeroNonce,
    string BaseTimestampText,
    string CoinbaseAmountText,
    DateTimeOffset CreatedUtc,
    DateTimeOffset ExpiresUtc,
    bool IsActive);

public sealed record ShareRecord(
    long ShareId,
    long RoundId,
    string PoolJobId,
    string MinerId,
    string NonceText,
    string TimestampText,
    string HashHex,
    double Difficulty,
    ShareStatus Status,
    bool MeetsBlockTarget,
    DateTimeOffset SubmittedUtc);

public sealed record FoundBlockRecord(
    string BlockId,
    long RoundId,
    string BlockHashHex,
    string HeightText,
    string RewardAtomicText,
    FoundBlockStatus Status,
    string ConfirmationsText,
    DateTimeOffset AcceptedUtc,
    DateTimeOffset? LastCheckedUtc,
    DateTimeOffset? FinalizedUtc,
    DateTimeOffset? OrphanedUtc);

public sealed record FoundBlockPayoutRecord(
    string PayoutId,
    string BlockId,
    long RoundId,
    string UserId,
    long AmountAtomic,
    FoundBlockPayoutStatus Status,
    DateTimeOffset CreatedUtc,
    DateTimeOffset? FinalizedUtc,
    DateTimeOffset? ReversedUtc);

public sealed record BalanceRecord(
    string UserId,
    long AvailableAtomic,
    long PendingWithdrawalAtomic,
    long TotalMinedAtomic,
    long TotalDepositedAtomic,
    long TotalWithdrawnAtomic,
    DateTimeOffset UpdatedUtc);

public sealed record LedgerEntryRecord(
    string LedgerEntryId,
    string UserId,
    LedgerEntryType EntryType,
    long DeltaAtomic,
    string Reference,
    string MetadataJson,
    DateTimeOffset CreatedUtc);

public sealed record WithdrawalRequestRecord(
    string WithdrawalId,
    string UserId,
    long AmountAtomic,
    string DestinationAddressHex,
    WithdrawalStatus Status,
    string? AdminNote,
    string? ExternalTxId,
    DateTimeOffset RequestedUtc,
    DateTimeOffset UpdatedUtc);

public sealed record WalletContactRecord(
    string ContactId,
    string UserId,
    string Label,
    string AddressHex,
    DateTimeOffset CreatedUtc);

public sealed record QadoPayContactRecord(
    string ContactId,
    string UserId,
    string Label,
    string Username,
    DateTimeOffset CreatedUtc);

public sealed record WalletTransactionRecord(
    string WalletTransactionId,
    string UserId,
    string Direction,
    string CounterpartyAddressHex,
    long AmountAtomic,
    long FeeAtomic,
    string? Note,
    string? TxId,
    string Status,
    DateTimeOffset CreatedUtc);

public sealed record DashboardSnapshot(
    bool ServerRunning,
    string CurrentRoundHeight,
    string CurrentRoundPrevHash,
    int UserCount,
    int VerifiedMinerCount,
    int PendingWithdrawalCount,
    int OpenRoundShareCount,
    string TotalTrackedBalanceAtomic,
    string? PoolOnChainBalanceAtomic,
    string? PoolBalanceDeltaAtomic,
    string SpendableTrackedBalanceAtomic,
    string ImmatureMiningBalanceAtomic);

public sealed record MinerStatsSnapshot(
    string MinerId,
    string PublicKeyHex,
    string Username,
    double ShareDifficulty,
    int AcceptedSharesRound,
    int StaleSharesRound,
    int InvalidSharesRound,
    string RoundId,
    string EstimatedHashrateText,
    string PoolHashrateText,
    string? NetworkHashrateText,
    DateTimeOffset? LastShareUtc);

public sealed record UserBalanceView(
    string UserId,
    string Username,
    string DepositAddressHex,
    string? WithdrawalAddressHex,
    long AvailableAtomic,
    long PendingWithdrawalAtomic,
    long TotalMinedAtomic,
    long TotalDepositedAtomic,
    long TotalWithdrawnAtomic,
    long ImmatureMiningAtomic);
