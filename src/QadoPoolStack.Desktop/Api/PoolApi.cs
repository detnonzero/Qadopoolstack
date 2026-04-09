using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.RateLimiting;
using QadoPoolStack.Desktop.Api.Models;
using QadoPoolStack.Desktop.Configuration;
using QadoPoolStack.Desktop.Domain;
using QadoPoolStack.Desktop.Hosting;
using QadoPoolStack.Desktop.Infrastructure.Logging;
using QadoPoolStack.Desktop.Persistence;
using QadoPoolStack.Desktop.Services.Accounts;
using QadoPoolStack.Desktop.Services.Mining;
using System.Text.Json;
using QadoPoolStack.Desktop.Utilities;

namespace QadoPoolStack.Desktop.Api;

public static class PoolApi
{
    public static void Map(WebApplication app)
    {
        app.MapGet("/public/config", (PoolSettings settings) =>
            Results.Ok(new PublicPoolConfigResponse(settings.AccountRegistrationEnabled)));

        app.MapGet("/health", () => Results.Ok(new
        {
            status = "ok",
            service = "qado-pool",
            timestampUtc = DateTimeOffset.UtcNow
        }));

        app.MapPost("/user/register", async (RegisterRequest request, UserAccountService accounts, CustodianWalletService walletService, MinerAuthService minerAuth, PoolRepository repository, PoolLogger logger, PoolSettings settings, CancellationToken ct) =>
        {
            try
            {
                var result = await accounts.RegisterAsync(request.Username, request.Password, settings, ct).ConfigureAwait(false);
                try
                {
                    _ = await EnsureAccountMiningReadyAsync(result.User, walletService, minerAuth, repository, settings, ct).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    logger.Warn("Auth", $"Automatic miner provisioning after register failed for user={result.User.Username}: {ex.Message}");
                }

                return Results.Ok(new AuthResponse(result.User.Username, result.SessionToken, GetPoolDepositAddress(settings)));
            }
            catch (Exception ex)
            {
                return Results.BadRequest(new { error = ex.Message });
            }
        }).RequireRateLimiting(PoolRateLimiting.PublicAuthPolicy);

        app.MapPost("/user/login", async (LoginRequest request, UserAccountService accounts, CustodianWalletService walletService, MinerAuthService minerAuth, PoolRepository repository, PoolLogger logger, PoolSettings settings, CancellationToken ct) =>
        {
            try
            {
                var result = await accounts.LoginAsync(request.Username, request.Password, settings, ct).ConfigureAwait(false);
                try
                {
                    _ = await EnsureAccountMiningReadyAsync(result.User, walletService, minerAuth, repository, settings, ct).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    logger.Warn("Auth", $"Automatic miner provisioning after login failed for user={result.User.Username}: {ex.Message}");
                }

                return Results.Ok(new AuthResponse(result.User.Username, result.SessionToken, GetPoolDepositAddress(settings)));
            }
            catch (Exception ex)
            {
                return Results.BadRequest(new { error = ex.Message });
            }
        }).RequireRateLimiting(PoolRateLimiting.PublicAuthPolicy);

        app.MapGet("/user/me", async (SessionService sessions, UserAccountService accounts, CustodianWalletService walletService, MinerAuthService minerAuth, PoolRepository repository, PoolSettings settings, HttpContext http, CancellationToken ct) =>
        {
            var user = await RequireUserAsync(http, sessions, settings, ct).ConfigureAwait(false);
            if (user is null)
            {
                return Results.Unauthorized();
            }

            (user, var miner) = await EnsureAccountMiningReadyAsync(user, walletService, minerAuth, repository, settings, ct).ConfigureAwait(false);
            var balance = await accounts.GetBalanceAsync(user.UserId, ct).ConfigureAwait(false);
            var immatureMining = await repository.GetUserImmatureMiningAtomicAsync(user.UserId, ct).ConfigureAwait(false);
            var pendingDeposits = await repository.GetPendingIncomingDepositAtomicForUserAsync(user.UserId, settings.DepositMinConfirmations, ct).ConfigureAwait(false);
            return Results.Ok(new MeResponse(
                user.Username,
                GetPoolDepositAddress(settings),
                user.WithdrawalAddressHex ?? miner?.PublicKeyHex,
                ToBalanceDto(balance, immatureMining, pendingDeposits),
                ToPoolFeePercent(settings.PoolFeeBasisPoints),
                miner?.PublicKeyHex,
                miner is null ? null : DifficultyCalibration.ToCalibratedDifficulty(miner.ShareDifficulty, settings),
                miner?.ApiTokenText,
                user.CustodianPublicKeyHex));
        }).RequireRateLimiting(PoolRateLimiting.UserApiPolicy);

        app.MapPost("/miner/bind", async (SessionService sessions, MinerAuthService minerAuth, PoolSettings settings, HttpContext http, CancellationToken ct) =>
        {
            var user = await RequireUserAsync(http, sessions, settings, ct).ConfigureAwait(false);
            if (user is null)
            {
                return Results.Unauthorized();
            }

            try
            {
                var result = await minerAuth.BindCustodianWalletAsync(user, settings, ct).ConfigureAwait(false);
                return Results.Ok(new MinerAuthResponse(result.Miner.PublicKeyHex, result.ApiToken, DifficultyCalibration.ToCalibratedDifficulty(result.Miner.ShareDifficulty, settings)));
            }
            catch (Exception ex)
            {
                return Results.BadRequest(new { error = ex.Message });
            }
        }).RequireRateLimiting(PoolRateLimiting.UserApiPolicy);

        app.MapGet("/wallet/summary", async (SessionService sessions, CustodianWalletService walletService, MinerAuthService minerAuth, PoolRepository repository, PoolSettings settings, HttpContext http, CancellationToken ct) =>
        {
            var user = await RequireUserAsync(http, sessions, settings, ct).ConfigureAwait(false);
            if (user is null)
            {
                return Results.Unauthorized();
            }

            try
            {
                (user, _) = await EnsureAccountMiningReadyAsync(user, walletService, minerAuth, repository, settings, ct).ConfigureAwait(false);
                var wallet = await walletService.GetSnapshotAsync(user, ct).ConfigureAwait(false);
                return Results.Ok(ToWalletSummaryDto(wallet));
            }
            catch (Exception ex)
            {
                return Results.BadRequest(new { error = ex.Message });
            }
        }).RequireRateLimiting(PoolRateLimiting.UserApiPolicy);

        app.MapPost("/wallet/keypair", async (SessionService sessions, CustodianWalletService walletService, PoolSettings settings, HttpContext http, CancellationToken ct) =>
        {
            var user = await RequireUserAsync(http, sessions, settings, ct).ConfigureAwait(false);
            if (user is null)
            {
                return Results.Unauthorized();
            }

            try
            {
                var updatedUser = await walletService.CreateKeyPairAsync(user, ct).ConfigureAwait(false);
                return Results.Ok(new WalletKeyPairResponse(updatedUser.CustodianPublicKeyHex ?? string.Empty));
            }
            catch (Exception ex)
            {
                return Results.BadRequest(new { error = ex.Message });
            }
        }).RequireRateLimiting(PoolRateLimiting.UserApiPolicy);

        app.MapPost("/wallet/send", async (WalletSendRequest request, SessionService sessions, CustodianWalletService walletService, PoolSettings settings, HttpContext http, CancellationToken ct) =>
        {
            var user = await RequireUserAsync(http, sessions, settings, ct).ConfigureAwait(false);
            if (user is null)
            {
                return Results.Unauthorized();
            }

            if (!AmountUtility.TryParseToAtomic(request.Amount, out var amountAtomic))
            {
                return Results.BadRequest(new { error = "Invalid amount." });
            }

            var feeText = string.IsNullOrWhiteSpace(request.Fee) ? "0" : request.Fee;
            if (!AmountUtility.TryParseToAtomic(feeText, out var feeAtomic))
            {
                return Results.BadRequest(new { error = "Invalid fee." });
            }

            try
            {
                var result = await walletService.SendTransactionAsync(user, request.Address, amountAtomic, feeAtomic, request.Note, ct).ConfigureAwait(false);
                return Results.Ok(new WalletSendResponse(
                    result.TxId,
                    result.RecipientAddressHex,
                    AmountUtility.FormatAtomic(result.AmountAtomic),
                    AmountUtility.FormatAtomic(result.FeeAtomic)));
            }
            catch (Exception ex)
            {
                return Results.BadRequest(new { error = ex.Message });
            }
        }).RequireRateLimiting(PoolRateLimiting.UserApiPolicy);

        app.MapPost("/wallet/address-book", async (WalletAddressBookRequest request, SessionService sessions, CustodianWalletService walletService, PoolSettings settings, HttpContext http, CancellationToken ct) =>
        {
            var user = await RequireUserAsync(http, sessions, settings, ct).ConfigureAwait(false);
            if (user is null)
            {
                return Results.Unauthorized();
            }

            try
            {
                var item = await walletService.AddAddressBookEntryAsync(user, request.Label, request.Address, ct).ConfigureAwait(false);
                return Results.Ok(ToWalletAddressBookDto(item));
            }
            catch (Exception ex)
            {
                return Results.BadRequest(new { error = ex.Message });
            }
        }).RequireRateLimiting(PoolRateLimiting.UserApiPolicy);

        app.MapDelete("/wallet/address-book/{contactId}", async (string contactId, SessionService sessions, CustodianWalletService walletService, PoolSettings settings, HttpContext http, CancellationToken ct) =>
        {
            var user = await RequireUserAsync(http, sessions, settings, ct).ConfigureAwait(false);
            if (user is null)
            {
                return Results.Unauthorized();
            }

            try
            {
                await walletService.DeleteAddressBookEntryAsync(user, contactId, ct).ConfigureAwait(false);
                return Results.Ok(new { status = "ok" });
            }
            catch (Exception ex)
            {
                return Results.BadRequest(new { error = ex.Message });
            }
        }).RequireRateLimiting(PoolRateLimiting.UserApiPolicy);

        app.MapGet("/qado-pay/summary", async (SessionService sessions, QadoPayService qadoPayService, PoolSettings settings, HttpContext http, CancellationToken ct) =>
        {
            var user = await RequireUserAsync(http, sessions, settings, ct).ConfigureAwait(false);
            if (user is null)
            {
                return Results.Unauthorized();
            }

            try
            {
                var summary = await qadoPayService.GetSnapshotAsync(user, ct).ConfigureAwait(false);
                return Results.Ok(new QadoPaySummaryResponse(
                    summary.Contacts.Select(ToQadoPayAddressBookDto).ToArray(),
                    summary.Payments.Select(ToQadoPayPaymentDto).ToArray(),
                    ToQadoPayOverviewDto(summary.Overview)));
            }
            catch (Exception ex)
            {
                return Results.BadRequest(new { error = ex.Message });
            }
        }).RequireRateLimiting(PoolRateLimiting.UserApiPolicy);

        app.MapPost("/qado-pay/address-book", async (QadoPayAddressBookRequest request, SessionService sessions, QadoPayService qadoPayService, PoolSettings settings, HttpContext http, CancellationToken ct) =>
        {
            var user = await RequireUserAsync(http, sessions, settings, ct).ConfigureAwait(false);
            if (user is null)
            {
                return Results.Unauthorized();
            }

            try
            {
                var item = await qadoPayService.AddAddressBookEntryAsync(user, request.Label, request.Username, ct).ConfigureAwait(false);
                return Results.Ok(ToQadoPayAddressBookDto(item));
            }
            catch (Exception ex)
            {
                return Results.BadRequest(new { error = ex.Message });
            }
        }).RequireRateLimiting(PoolRateLimiting.UserApiPolicy);

        app.MapDelete("/qado-pay/address-book/{contactId}", async (string contactId, SessionService sessions, QadoPayService qadoPayService, PoolSettings settings, HttpContext http, CancellationToken ct) =>
        {
            var user = await RequireUserAsync(http, sessions, settings, ct).ConfigureAwait(false);
            if (user is null)
            {
                return Results.Unauthorized();
            }

            try
            {
                await qadoPayService.DeleteAddressBookEntryAsync(user, contactId, ct).ConfigureAwait(false);
                return Results.Ok(new { status = "ok" });
            }
            catch (Exception ex)
            {
                return Results.BadRequest(new { error = ex.Message });
            }
        }).RequireRateLimiting(PoolRateLimiting.UserApiPolicy);

        app.MapGet("/mining/job", async (HttpContext http, MinerAuthService minerAuth, MiningJobService miningJobs, PoolSettings settings, CancellationToken ct) =>
        {
            var miner = await RequireMinerAsync(http, minerAuth, ct).ConfigureAwait(false);
            if (miner is null)
            {
                return Results.Unauthorized();
            }

            try
            {
                var envelope = await miningJobs.CreateMinerJobAsync(miner, settings, ct).ConfigureAwait(false);
                var nodeJob = envelope.NodeJob;
                return Results.Ok(new PoolJobResponse(
                    envelope.PoolJob.PoolJobId,
                    nodeJob.Height,
                    nodeJob.PrevHash,
                    nodeJob.Target,
                    envelope.PoolJob.ShareTargetHex,
                    nodeJob.Timestamp,
                    nodeJob.MerkleRoot,
                    nodeJob.CoinbaseAmount,
                    nodeJob.TxCount,
                    nodeJob.HeaderHexZeroNonce,
                    nodeJob.PrecomputedCv,
                    nodeJob.Block1Base,
                    nodeJob.Block2,
                    nodeJob.TargetWords));
            }
            catch (Exception ex)
            {
                return Results.BadRequest(new { error = ex.Message });
            }
        }).RequireRateLimiting(PoolRateLimiting.MinerApiPolicy);

        app.MapPost("/mining/submit-share", async (MiningShareSubmitRequest request, HttpContext http, MinerAuthService minerAuth, ShareValidationService shareValidation, PoolSettings settings, CancellationToken ct) =>
        {
            var miner = await RequireMinerAsync(http, minerAuth, ct).ConfigureAwait(false);
            if (miner is null)
            {
                return Results.Unauthorized();
            }

            try
            {
                var result = await shareValidation.SubmitShareAsync(miner, request.JobId, request.Nonce, request.Timestamp, settings, ct).ConfigureAwait(false);
                return Results.Ok(new ShareSubmitResponse(result.Accepted, result.Duplicate, result.Stale, result.BlockCandidate, result.BlockAccepted, result.HashHex, result.Reason, result.ReloadJob, DifficultyCalibration.ToCalibratedDifficulty(result.ShareDifficulty, settings)));
            }
            catch (Exception ex)
            {
                return Results.BadRequest(new { error = ex.Message });
            }
        }).RequireRateLimiting(PoolRateLimiting.ShareSubmitPolicy);

        app.MapGet("/miner/stats", async (HttpContext http, MinerAuthService minerAuth, DifficultyService difficultyService, PoolSettings settings, CancellationToken ct) =>
        {
            var miner = await RequireMinerAsync(http, minerAuth, ct).ConfigureAwait(false);
            if (miner is null)
            {
                return Results.Unauthorized();
            }

            var stats = await difficultyService.GetMinerStatsAsync(miner, ct).ConfigureAwait(false);
            return Results.Ok(new MinerStatsResponse(
                stats.PublicKeyHex,
                stats.Username,
                DifficultyCalibration.ToCalibratedDifficulty(stats.ShareDifficulty, settings),
                stats.AcceptedSharesRound,
                stats.StaleSharesRound,
                stats.InvalidSharesRound,
                stats.RoundId,
                stats.EstimatedHashrateText,
                stats.PoolHashrateText,
                stats.NetworkHashrateText,
                stats.LastShareUtc));
        }).RequireRateLimiting(PoolRateLimiting.MinerApiPolicy);

        app.MapMethods("/deposit", ["GET", "POST"], async (SessionService sessions, UserAccountService accounts, PoolRepository repository, PoolSettings settings, HttpContext http, CancellationToken ct) =>
        {
            var user = await RequireUserAsync(http, sessions, settings, ct).ConfigureAwait(false);
            if (user is null)
            {
                return Results.Unauthorized();
            }

            var balance = await accounts.GetBalanceAsync(user.UserId, ct).ConfigureAwait(false);
            var immatureMining = await repository.GetUserImmatureMiningAtomicAsync(user.UserId, ct).ConfigureAwait(false);
            var pendingDeposits = await repository.GetPendingIncomingDepositAtomicForUserAsync(user.UserId, settings.DepositMinConfirmations, ct).ConfigureAwait(false);
            return Results.Ok(new DepositResponse(GetPoolDepositAddress(settings), ToBalanceDto(balance, immatureMining, pendingDeposits)));
        }).RequireRateLimiting(PoolRateLimiting.UserApiPolicy);

        app.MapPost("/withdraw", async (WithdrawRequest request, SessionService sessions, LedgerService ledgerService, PoolSettings settings, HttpContext http, CancellationToken ct) =>
        {
            var user = await RequireUserAsync(http, sessions, settings, ct).ConfigureAwait(false);
            if (user is null)
            {
                return Results.Unauthorized();
            }

            if (!AmountUtility.TryParseToAtomic(request.Amount, out var amountAtomic))
            {
                return Results.BadRequest(new { error = "Invalid amount." });
            }

            var feeText = string.IsNullOrWhiteSpace(request.Fee) ? "0" : request.Fee;
            if (!AmountUtility.TryParseToAtomic(feeText, out var feeAtomic))
            {
                return Results.BadRequest(new { error = "Invalid fee." });
            }

            try
            {
                var result = await ledgerService.RequestWithdrawalAsync(user, amountAtomic, feeAtomic, ct).ConfigureAwait(false);
                return Results.Ok(new WithdrawResponse(
                    result.WithdrawalId,
                    result.Status.ToString(),
                    result.ExternalTxId,
                    AmountUtility.FormatAtomic(amountAtomic - feeAtomic),
                    AmountUtility.FormatAtomic(feeAtomic)));
            }
            catch (Exception ex)
            {
                return Results.BadRequest(new { error = ex.Message });
            }
        }).RequireRateLimiting(PoolRateLimiting.UserApiPolicy);

        app.MapPost("/ledger/transfer", async (TransferRequest request, SessionService sessions, LedgerService ledgerService, PoolSettings settings, HttpContext http, CancellationToken ct) =>
        {
            var user = await RequireUserAsync(http, sessions, settings, ct).ConfigureAwait(false);
            if (user is null)
            {
                return Results.Unauthorized();
            }

            if (!AmountUtility.TryParseToAtomic(request.Amount, out var amountAtomic))
            {
                return Results.BadRequest(new { error = "Invalid amount." });
            }

            try
            {
                await ledgerService.TransferInternalAsync(user.UserId, request.Username, amountAtomic, request.Note ?? "", ct).ConfigureAwait(false);
                return Results.Ok(new TransferResponse("ok"));
            }
            catch (Exception ex)
            {
                return Results.BadRequest(new { error = ex.Message });
            }
        }).RequireRateLimiting(PoolRateLimiting.UserApiPolicy);

        app.MapGet("/ledger/history", async (SessionService sessions, PoolRepository repository, PoolSettings settings, HttpContext http, CancellationToken ct) =>
        {
            var user = await RequireUserAsync(http, sessions, settings, ct).ConfigureAwait(false);
            if (user is null)
            {
                return Results.Unauthorized();
            }

            var items = await repository.ListLedgerEntriesByUserIdAsync(user.UserId, ct).ConfigureAwait(false);
            return Results.Ok(items
                .Where(ShouldIncludeInHistory)
                .Select(ToLedgerHistoryDto)
                .ToArray());
        }).RequireRateLimiting(PoolRateLimiting.UserApiPolicy);
    }

    private static async Task<QadoPoolStack.Desktop.Domain.PoolUser?> RequireUserAsync(HttpContext http, SessionService sessions, PoolSettings settings, CancellationToken cancellationToken)
    {
        var token = GetBearerToken(http);
        return await sessions.GetUserFromTokenAsync(token, settings, cancellationToken).ConfigureAwait(false);
    }

    private static async Task<QadoPoolStack.Desktop.Domain.MinerRecord?> RequireMinerAsync(HttpContext http, MinerAuthService minerAuth, CancellationToken cancellationToken)
    {
        var token = http.Request.Headers.Authorization.ToString();
        if (token.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase))
        {
            token = token["Bearer ".Length..].Trim();
        }
        else
        {
            token = http.Request.Headers["X-Miner-Token"].ToString().Trim();
        }

        return await minerAuth.GetMinerFromApiTokenAsync(token, cancellationToken).ConfigureAwait(false);
    }

    private static async Task<(PoolUser User, MinerRecord? Miner)> EnsureAccountMiningReadyAsync(
        PoolUser user,
        CustodianWalletService walletService,
        MinerAuthService minerAuth,
        PoolRepository repository,
        PoolSettings settings,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(user.CustodianPublicKeyHex))
        {
            user = await walletService.CreateKeyPairAsync(user, cancellationToken).ConfigureAwait(false);
        }

        MinerRecord? miner = await repository.GetMinerByUserIdAsync(user.UserId, cancellationToken).ConfigureAwait(false);
        var expectedPublicKey = string.IsNullOrWhiteSpace(user.CustodianPublicKeyHex)
            ? null
            : HexUtility.NormalizeLower(user.CustodianPublicKeyHex, 32);

        var needsBinding = !string.IsNullOrWhiteSpace(expectedPublicKey) &&
            (miner is null
             || !miner.IsVerified
             || !string.Equals(miner.PublicKeyHex, expectedPublicKey, StringComparison.Ordinal)
             || string.IsNullOrWhiteSpace(miner.ApiTokenText));

        if (needsBinding)
        {
            var result = await minerAuth.BindCustodianWalletAsync(user, settings, cancellationToken).ConfigureAwait(false);
            miner = result.Miner;
            user = await repository.GetUserByIdAsync(user.UserId, cancellationToken).ConfigureAwait(false)
                ?? throw new InvalidOperationException("Account owner no longer exists.");
        }

        return (user, miner);
    }

    private static string? GetBearerToken(HttpContext http)
    {
        var authorization = http.Request.Headers.Authorization.ToString();
        if (authorization.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase))
        {
            return authorization["Bearer ".Length..].Trim();
        }

        return null;
    }

    private static BalanceDto ToBalanceDto(QadoPoolStack.Desktop.Domain.BalanceRecord? balance, long immatureMiningAtomic, long pendingDepositAtomic)
    {
        return new BalanceDto(
            AmountUtility.FormatAtomic(balance?.AvailableAtomic ?? 0),
            AmountUtility.FormatAtomic(balance?.PendingWithdrawalAtomic ?? 0),
            AmountUtility.FormatAtomic(pendingDepositAtomic),
            AmountUtility.FormatAtomic(balance?.TotalMinedAtomic ?? 0),
            AmountUtility.FormatAtomic(balance?.TotalDepositedAtomic ?? 0),
            AmountUtility.FormatAtomic(balance?.TotalWithdrawnAtomic ?? 0),
            AmountUtility.FormatAtomic(immatureMiningAtomic));
    }

    private static string ToPoolFeePercent(int basisPoints)
        => $"{basisPoints / 100d:0.00}%";

    private static string GetPoolDepositAddress(PoolSettings settings)
        => string.IsNullOrWhiteSpace(settings.PoolMinerPublicKey) ? "" : settings.PoolMinerPublicKey;

    private static WalletSummaryResponse ToWalletSummaryDto(CustodianWalletSnapshot wallet)
    {
        return new WalletSummaryResponse(
            !string.IsNullOrWhiteSpace(wallet.PublicKeyHex),
            wallet.PublicKeyHex,
            new WalletBalanceResponse(
                AmountUtility.FormatAtomic(wallet.BalanceAtomic),
                wallet.PendingOutgoingCount,
                wallet.PendingIncomingCount),
            wallet.Transactions.Select(ToWalletTransactionDto).ToArray(),
            wallet.Contacts.Select(ToWalletAddressBookDto).ToArray());
    }

    private static WalletTransactionItemResponse ToWalletTransactionDto(CustodianWalletTimelineItem item)
    {
        return new WalletTransactionItemResponse(
            item.TransactionId,
            item.Direction,
            item.CounterpartyAddressHex,
            AmountUtility.FormatAtomic(item.AmountAtomic),
            AmountUtility.FormatAtomic(item.FeeAtomic),
            item.Note,
            item.TxId,
            item.Status,
            item.CreatedUtc);
    }

    private static WalletAddressBookItemResponse ToWalletAddressBookDto(WalletContactRecord item)
    {
        return new WalletAddressBookItemResponse(
            item.ContactId,
            item.Label,
            item.AddressHex,
            item.CreatedUtc);
    }

    private static QadoPayAddressBookItemResponse ToQadoPayAddressBookDto(QadoPayContactRecord item)
    {
        return new QadoPayAddressBookItemResponse(
            item.ContactId,
            item.Label,
            item.Username,
            item.CreatedUtc);
    }

    private static QadoPayPaymentItemResponse ToQadoPayPaymentDto(LedgerEntryRecord entry)
    {
        var metadata = TryParseMetadata(entry.MetadataJson);
        var isOutgoing = entry.EntryType == LedgerEntryType.InternalTransferOut;
        var username = isOutgoing
            ? ReadMetadataString(metadata, "to") ?? StripReferencePrefix(entry.Reference, "transfer:") ?? "-"
            : ReadMetadataString(metadata, "from") ?? StripReferencePrefix(entry.Reference, "transfer:") ?? "-";

        return new QadoPayPaymentItemResponse(
            entry.LedgerEntryId,
            isOutgoing ? "outgoing" : "incoming",
            username,
            AmountUtility.FormatAtomic(Math.Abs(entry.DeltaAtomic)),
            string.IsNullOrWhiteSpace(ReadMetadataString(metadata, "note")) ? null : ReadMetadataString(metadata, "note"),
            entry.CreatedUtc);
    }

    private static QadoPayOverviewResponse ToQadoPayOverviewDto(QadoPayOverview overview)
    {
        return new QadoPayOverviewResponse(
            AmountUtility.FormatAtomic(overview.SentTodayAtomic),
            AmountUtility.FormatAtomic(overview.ReceivedTodayAtomic),
            AmountUtility.FormatAtomic(overview.ReceivedTodayAtomic - overview.SentTodayAtomic),
            overview.LastPayment is null ? null : ToQadoPayLastPaymentDto(overview.LastPayment));
    }

    private static QadoPayLastPaymentResponse ToQadoPayLastPaymentDto(LedgerEntryRecord entry)
    {
        var metadata = TryParseMetadata(entry.MetadataJson);
        var isOutgoing = entry.EntryType == LedgerEntryType.InternalTransferOut;
        var username = isOutgoing
            ? ReadMetadataString(metadata, "to") ?? StripReferencePrefix(entry.Reference, "transfer:") ?? "-"
            : ReadMetadataString(metadata, "from") ?? StripReferencePrefix(entry.Reference, "transfer:") ?? "-";

        return new QadoPayLastPaymentResponse(
            isOutgoing ? "outgoing" : "incoming",
            username,
            AmountUtility.FormatAtomic(Math.Abs(entry.DeltaAtomic)),
            entry.CreatedUtc);
    }

    private static bool ShouldIncludeInHistory(LedgerEntryRecord entry)
        => entry.EntryType is LedgerEntryType.DepositCredit
            or LedgerEntryType.BlockReward
            or LedgerEntryType.WithdrawalReserve;

    private static LedgerHistoryItemResponse ToLedgerHistoryDto(LedgerEntryRecord entry)
    {
        var metadata = TryParseMetadata(entry.MetadataJson);
        var kind = entry.EntryType switch
        {
            LedgerEntryType.DepositCredit => "Deposit",
            LedgerEntryType.BlockReward => "Mining reward",
            LedgerEntryType.InternalTransferOut => "Payment sent",
            LedgerEntryType.InternalTransferIn => "Payment received",
            LedgerEntryType.WithdrawalReserve => "Withdrawal",
            LedgerEntryType.WithdrawalRelease when entry.DeltaAtomic > 0 => "Withdrawal reversed",
            LedgerEntryType.WithdrawalRelease => "Withdrawal update",
            LedgerEntryType.ManualAdjustment when entry.Reference.StartsWith("block-reward-reverse:", StringComparison.Ordinal) => "Mining reward reversed",
            LedgerEntryType.ManualAdjustment when entry.DeltaAtomic < 0 => "Debit adjustment",
            LedgerEntryType.ManualAdjustment => "Credit adjustment",
            _ => entry.EntryType.ToString()
        };

        var (counterpartyLabel, counterparty) = entry.EntryType switch
        {
            LedgerEntryType.InternalTransferOut => ("Recipient", ReadMetadataString(metadata, "to") ?? StripReferencePrefix(entry.Reference, "transfer:") ?? "-"),
            LedgerEntryType.InternalTransferIn => ("Sender", ReadMetadataString(metadata, "from") ?? StripReferencePrefix(entry.Reference, "transfer:") ?? "-"),
            LedgerEntryType.DepositCredit => ("Sender", ReadMetadataString(metadata, "matchedSenderPublicKey") ?? ReadMetadataString(metadata, "fromAddress") ?? ReadFirstMetadataArrayString(metadata, "fromAddresses") ?? "-"),
            LedgerEntryType.BlockReward => ("Source", "Pool block reward"),
            LedgerEntryType.WithdrawalReserve => ("Recipient", ReadMetadataString(metadata, "address") ?? "-"),
            LedgerEntryType.WithdrawalRelease when entry.DeltaAtomic > 0 => ("Source", "Pool accounting"),
            LedgerEntryType.WithdrawalRelease => ("Recipient", ReadMetadataString(metadata, "address") ?? "-"),
            LedgerEntryType.ManualAdjustment when entry.Reference.StartsWith("block-reward-reverse:", StringComparison.Ordinal) => ("Source", "Pool reorg reconciliation"),
            LedgerEntryType.ManualAdjustment => ("Reference", string.IsNullOrWhiteSpace(entry.Reference) ? "-" : entry.Reference),
            _ => ("Reference", string.IsNullOrWhiteSpace(entry.Reference) ? "-" : entry.Reference)
        };

        var note = entry.EntryType switch
        {
            LedgerEntryType.InternalTransferOut or LedgerEntryType.InternalTransferIn => ReadMetadataString(metadata, "note"),
            LedgerEntryType.DepositCredit => ReadMetadataString(metadata, "blockHeight") is { } blockHeight ? $"Block {blockHeight}" : null,
            LedgerEntryType.BlockReward => ReadMetadataString(metadata, "height") is { } rewardHeight ? $"Block {rewardHeight}" : null,
            LedgerEntryType.WithdrawalReserve => BuildWithdrawalNote(metadata),
            LedgerEntryType.WithdrawalRelease => ReadMetadataString(metadata, "note"),
            LedgerEntryType.ManualAdjustment when entry.Reference.StartsWith("block-reward-reverse:", StringComparison.Ordinal)
                => ReadMetadataString(metadata, "height") is { } orphanedHeight ? $"Orphaned block {orphanedHeight}" : ReadMetadataString(metadata, "reason"),
            LedgerEntryType.ManualAdjustment => ReadMetadataString(metadata, "note"),
            _ => null
        };

        var txId = entry.EntryType switch
        {
            LedgerEntryType.DepositCredit => ReadMetadataString(metadata, "txid"),
            LedgerEntryType.WithdrawalReserve => ReadMetadataString(metadata, "txid"),
            LedgerEntryType.WithdrawalRelease => ReadMetadataString(metadata, "txid"),
            _ => null
        };

        return new LedgerHistoryItemResponse(
            entry.LedgerEntryId,
            kind,
            counterpartyLabel,
            counterparty,
            AmountUtility.FormatAtomic(Math.Abs(entry.DeltaAtomic)),
            entry.DeltaAtomic < 0,
            string.IsNullOrWhiteSpace(note) ? null : note,
            string.IsNullOrWhiteSpace(txId) ? null : txId,
            entry.CreatedUtc);
    }

    private static string? BuildWithdrawalNote(JsonElement? metadata)
    {
        var sentAmount = ReadMetadataString(metadata, "sentAmount");
        var fee = ReadMetadataString(metadata, "fee");

        if (!string.IsNullOrWhiteSpace(sentAmount) && !string.IsNullOrWhiteSpace(fee))
        {
            return $"Sent {sentAmount} QADO after fee {fee} QADO";
        }

        if (!string.IsNullOrWhiteSpace(fee))
        {
            return $"Fee {fee} QADO";
        }

        return null;
    }

    private static JsonElement? TryParseMetadata(string? metadataJson)
    {
        if (string.IsNullOrWhiteSpace(metadataJson))
        {
            return null;
        }

        try
        {
            using var document = JsonDocument.Parse(metadataJson);
            return document.RootElement.Clone();
        }
        catch
        {
            return null;
        }
    }

    private static string? ReadMetadataString(JsonElement? metadata, string propertyName)
    {
        if (metadata is not JsonElement element || !element.TryGetProperty(propertyName, out var property))
        {
            return null;
        }

        return property.ValueKind switch
        {
            JsonValueKind.String => property.GetString(),
            JsonValueKind.Number => property.ToString(),
            JsonValueKind.True => bool.TrueString,
            JsonValueKind.False => bool.FalseString,
            _ => null
        };
    }

    private static string? ReadFirstMetadataArrayString(JsonElement? metadata, string propertyName)
    {
        if (metadata is not JsonElement element || !element.TryGetProperty(propertyName, out var property) || property.ValueKind != JsonValueKind.Array)
        {
            return null;
        }

        foreach (var item in property.EnumerateArray())
        {
            if (item.ValueKind == JsonValueKind.String && !string.IsNullOrWhiteSpace(item.GetString()))
            {
                return item.GetString();
            }
        }

        return null;
    }

    private static string? StripReferencePrefix(string? value, string prefix)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        return value.StartsWith(prefix, StringComparison.Ordinal)
            ? value[prefix.Length..]
            : value;
    }
}
