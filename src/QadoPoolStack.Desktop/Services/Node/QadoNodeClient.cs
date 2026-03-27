using System.Net.Http;
using System.Net.Http.Json;
using System.Text.Json.Serialization;
using QadoPoolStack.Desktop.Infrastructure.Logging;

namespace QadoPoolStack.Desktop.Services.Node;

public sealed partial class QadoNodeClient
{
    private readonly HttpClient _httpClient;
    private readonly PoolLogger _logger;

    public QadoNodeClient(HttpClient httpClient, PoolLogger logger)
    {
        _httpClient = httpClient;
        _logger = logger;
    }

    public async Task<QadoMiningJobResponse> CreateMiningJobAsync(string minerPublicKeyHex, CancellationToken cancellationToken = default)
    {
        var response = await _httpClient.PostAsJsonAsync("/v1/mining/job", new QadoMiningJobRequest(minerPublicKeyHex), cancellationToken).ConfigureAwait(false);
        return await ReadJsonAsync<QadoMiningJobResponse>(response, "create mining job", cancellationToken).ConfigureAwait(false);
    }

    public async Task<QadoMiningSubmitResponse> SubmitMiningAsync(string jobId, string nonceText, string? timestampText, CancellationToken cancellationToken = default)
    {
        var response = await _httpClient.PostAsJsonAsync("/v1/mining/submit", new QadoMiningSubmitRequest(jobId, nonceText, timestampText), cancellationToken).ConfigureAwait(false);
        return await ReadJsonAsync<QadoMiningSubmitResponse>(response, "submit mining candidate", cancellationToken).ConfigureAwait(false);
    }

    public async Task<QadoAddressResponse?> GetAddressAsync(string addressHex, CancellationToken cancellationToken = default)
    {
        var response = await _httpClient.GetAsync($"/v1/address/{addressHex}", cancellationToken).ConfigureAwait(false);
        if (response.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return null;
        }

        return await ReadJsonAsync<QadoAddressResponse>(response, "get address state", cancellationToken).ConfigureAwait(false);
    }

    public async Task<QadoTipResponse> GetTipAsync(CancellationToken cancellationToken = default)
    {
        var response = await _httpClient.GetAsync("/v1/tip", cancellationToken).ConfigureAwait(false);
        return await ReadJsonAsync<QadoTipResponse>(response, "get chain tip", cancellationToken).ConfigureAwait(false);
    }

    public async Task<QadoBlockResponse?> GetBlockAsync(string blockRef, CancellationToken cancellationToken = default)
    {
        var response = await _httpClient.GetAsync($"/v1/block/{Uri.EscapeDataString(blockRef)}", cancellationToken).ConfigureAwait(false);
        if (response.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return null;
        }

        return await ReadJsonAsync<QadoBlockResponse>(response, "get block", cancellationToken).ConfigureAwait(false);
    }

    private async Task<T> ReadJsonAsync<T>(HttpResponseMessage response, string action, CancellationToken cancellationToken)
    {
        if (!response.IsSuccessStatusCode)
        {
            var body = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            _logger.Warn("Node", $"Qado node failed to {action}: {(int)response.StatusCode} {response.ReasonPhrase} {body}");
            throw new HttpRequestException($"Qado node failed to {action}: {(int)response.StatusCode} {response.ReasonPhrase}");
        }

        var payload = await response.Content.ReadFromJsonAsync<T>(cancellationToken: cancellationToken).ConfigureAwait(false);
        return payload ?? throw new InvalidOperationException($"Qado node returned an empty payload for {action}.");
    }
}

public sealed record QadoMiningJobRequest([property: JsonPropertyName("miner")] string Miner);

public sealed record QadoMiningSubmitRequest(
    [property: JsonPropertyName("job_id")] string JobId,
    [property: JsonPropertyName("nonce")] string Nonce,
    [property: JsonPropertyName("timestamp")] string? Timestamp);

public sealed record QadoMiningJobResponse
{
    [JsonPropertyName("job_id")]
    public string JobId { get; init; } = "";

    [JsonPropertyName("height")]
    public string Height { get; init; } = "0";

    [JsonPropertyName("prev_hash")]
    public string PrevHash { get; init; } = "";

    [JsonPropertyName("target")]
    public string Target { get; init; } = "";

    [JsonPropertyName("timestamp")]
    public string Timestamp { get; init; } = "0";

    [JsonPropertyName("merkle_root")]
    public string MerkleRoot { get; init; } = "";

    [JsonPropertyName("coinbase_amount")]
    public string CoinbaseAmount { get; init; } = "0";

    [JsonPropertyName("tx_count")]
    public int TxCount { get; init; }

    [JsonPropertyName("header_hex_zero_nonce")]
    public string HeaderHexZeroNonce { get; init; } = "";

    [JsonPropertyName("precomputed_cv")]
    public string PrecomputedCv { get; init; } = "";

    [JsonPropertyName("block1_base")]
    public string Block1Base { get; init; } = "";

    [JsonPropertyName("block2")]
    public string Block2 { get; init; } = "";

    [JsonPropertyName("target_words")]
    public string[] TargetWords { get; init; } = [];
}

public sealed record QadoMiningSubmitResponse
{
    [JsonPropertyName("accepted")]
    public bool Accepted { get; init; }

    [JsonPropertyName("hash")]
    public string? Hash { get; init; }

    [JsonPropertyName("height")]
    public string? Height { get; init; }

    [JsonPropertyName("reason")]
    public string? Reason { get; init; }
}

public sealed record QadoAddressResponse
{
    [JsonPropertyName("address")]
    public string Address { get; init; } = "";

    [JsonPropertyName("balance_atomic")]
    public string BalanceAtomic { get; init; } = "0";

    [JsonPropertyName("nonce")]
    public string Nonce { get; init; } = "0";

    [JsonPropertyName("pending_outgoing_count")]
    public int PendingOutgoingCount { get; init; }

    [JsonPropertyName("pending_incoming_count")]
    public int PendingIncomingCount { get; init; }

    [JsonPropertyName("latest_observed_height")]
    public string LatestObservedHeight { get; init; } = "0";
}

public sealed record QadoTipResponse
{
    [JsonPropertyName("height")]
    public string Height { get; init; } = "0";

    [JsonPropertyName("hash")]
    public string Hash { get; init; } = "";

    [JsonPropertyName("timestamp_utc")]
    public DateTimeOffset TimestampUtc { get; init; }

    [JsonPropertyName("chainwork")]
    public string Chainwork { get; init; } = "0";
}

public sealed record QadoBlockResponse
{
    [JsonPropertyName("hash")]
    public string Hash { get; init; } = "";

    [JsonPropertyName("height")]
    public string Height { get; init; } = "0";

    [JsonPropertyName("prev_hash")]
    public string PrevHash { get; init; } = "";

    [JsonPropertyName("timestamp_utc")]
    public DateTimeOffset TimestampUtc { get; init; }

    [JsonPropertyName("miner")]
    public string? Miner { get; init; }

    [JsonPropertyName("tx_count")]
    public int TxCount { get; init; }
}
