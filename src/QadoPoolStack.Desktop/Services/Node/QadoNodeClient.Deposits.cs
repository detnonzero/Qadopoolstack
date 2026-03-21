using System.Net.Http;
using System.Text.Json.Serialization;

namespace QadoPoolStack.Desktop.Services.Node;

public sealed partial class QadoNodeClient
{
    public async Task<QadoAddressIncomingResponse> GetIncomingAddressEventsAsync(
        string addressHex,
        string? cursor,
        int limit,
        int minConfirmations,
        CancellationToken cancellationToken = default)
    {
        var query = new List<string>
        {
            $"limit={Math.Clamp(limit, 1, 1000)}",
            $"min_confirmations={Math.Max(0, minConfirmations)}"
        };

        if (!string.IsNullOrWhiteSpace(cursor))
        {
            query.Add($"cursor={Uri.EscapeDataString(cursor)}");
        }

        var response = await _httpClient.GetAsync($"/v1/address/{addressHex}/incoming?{string.Join("&", query)}", cancellationToken).ConfigureAwait(false);
        return await ReadJsonAsync<QadoAddressIncomingResponse>(response, "get incoming address events", cancellationToken).ConfigureAwait(false);
    }
}

public sealed record QadoAddressIncomingResponse
{
    [JsonPropertyName("address")]
    public string Address { get; init; } = "";

    [JsonPropertyName("tip_height")]
    public string TipHeight { get; init; } = "0";

    [JsonPropertyName("next_cursor")]
    public string NextCursor { get; init; } = "";

    [JsonPropertyName("items")]
    public QadoIncomingAddressEvent[] Items { get; init; } = [];
}

public sealed record QadoIncomingAddressEvent
{
    [JsonPropertyName("event_id")]
    public string EventId { get; init; } = "";

    [JsonPropertyName("txid")]
    public string TxId { get; init; } = "";

    [JsonPropertyName("status")]
    public string Status { get; init; } = "";

    [JsonPropertyName("block_height")]
    public string BlockHeight { get; init; } = "0";

    [JsonPropertyName("block_hash")]
    public string BlockHash { get; init; } = "";

    [JsonPropertyName("confirmations")]
    public string Confirmations { get; init; } = "0";

    [JsonPropertyName("timestamp_utc")]
    public DateTimeOffset TimestampUtc { get; init; }

    [JsonPropertyName("to_address")]
    public string ToAddress { get; init; } = "";

    [JsonPropertyName("amount_atomic")]
    public string AmountAtomic { get; init; } = "0";

    [JsonPropertyName("from_address")]
    public string? FromAddress { get; init; }

    [JsonPropertyName("from_addresses")]
    public string[] FromAddresses { get; init; } = [];

    [JsonPropertyName("tx_index")]
    public int TxIndex { get; init; }

    [JsonPropertyName("transfer_index")]
    public int TransferIndex { get; init; }
}
