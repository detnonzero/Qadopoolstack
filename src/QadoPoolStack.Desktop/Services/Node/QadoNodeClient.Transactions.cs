using System.Net.Http.Json;
using System.Text.Json.Serialization;

namespace QadoPoolStack.Desktop.Services.Node;

public sealed partial class QadoNodeClient
{
    public async Task<QadoNetworkResponse> GetNetworkAsync(CancellationToken cancellationToken = default)
    {
        var response = await _httpClient.GetAsync("/v1/network", cancellationToken).ConfigureAwait(false);
        return await ReadJsonAsync<QadoNetworkResponse>(response, "get network metadata", cancellationToken).ConfigureAwait(false);
    }

    public async Task<QadoBroadcastTransactionResponse> BroadcastTransactionAsync(string rawTransactionHex, string? idempotencyKey, CancellationToken cancellationToken = default)
    {
        var response = await _httpClient.PostAsJsonAsync(
            "/v1/tx/broadcast",
            new QadoBroadcastTransactionRequest(rawTransactionHex, idempotencyKey),
            cancellationToken).ConfigureAwait(false);
        return await ReadJsonAsync<QadoBroadcastTransactionResponse>(response, "broadcast transaction", cancellationToken).ConfigureAwait(false);
    }
}

public sealed record QadoBroadcastTransactionRequest(
    [property: JsonPropertyName("raw_tx_hex")] string RawTransactionHex,
    [property: JsonPropertyName("idempotency_key")] string? IdempotencyKey);

public sealed record QadoBroadcastTransactionResponse
{
    [JsonPropertyName("accepted")]
    public bool Accepted { get; init; }

    [JsonPropertyName("txid")]
    public string? TxId { get; init; }

    [JsonPropertyName("status")]
    public string? Status { get; init; }

    [JsonPropertyName("error")]
    public string? Error { get; init; }
}

public sealed record QadoNetworkResponse
{
    [JsonPropertyName("chain_name")]
    public string ChainName { get; init; } = "";

    [JsonPropertyName("symbol")]
    public string Symbol { get; init; } = "";

    [JsonPropertyName("decimals")]
    public int Decimals { get; init; }

    [JsonPropertyName("chain_id")]
    public string ChainId { get; init; } = "0";

    [JsonPropertyName("network_id")]
    public string NetworkId { get; init; } = "0";

    [JsonPropertyName("p2p_port")]
    public int P2pPort { get; init; }

    [JsonPropertyName("genesis_hash")]
    public string GenesisHash { get; init; } = "";
}
