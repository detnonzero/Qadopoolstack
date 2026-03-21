using System.Buffers.Binary;
using Blake3;

namespace QadoPoolStack.Desktop.Utilities;

public static class QadoTransactionUtility
{
    private const byte FormatVersion = 2;

    public static string BuildSignedRawTransactionHex(
        uint chainId,
        string senderPrivateKeyHex,
        string recipientPublicKeyHex,
        ulong amountAtomic,
        ulong feeAtomic,
        ulong nonce)
    {
        var senderPublicKeyHex = KeyUtility.DeriveEd25519PublicKeyHex(senderPrivateKeyHex);
        var sender = HexUtility.Parse(senderPublicKeyHex, 32);
        var recipient = HexUtility.Parse(recipientPublicKeyHex, 32);
        var signingBytes = BuildSigningBytes(chainId, sender, recipient, amountAtomic, feeAtomic, nonce);
        var signature = KeyUtility.SignEd25519(senderPrivateKeyHex, signingBytes);
        var rawTransaction = BuildRawTransactionBytes(chainId, sender, recipient, amountAtomic, feeAtomic, nonce, signature);
        return HexUtility.ToLowerHex(rawTransaction);
    }

    public static string ComputeTransactionIdHex(string rawTransactionHex)
    {
        var rawTransaction = Convert.FromHexString(rawTransactionHex);
        var hash = new byte[32];
        Hasher.Hash(rawTransaction, hash);
        return HexUtility.ToLowerHex(hash);
    }

    private static byte[] BuildSigningBytes(
        uint chainId,
        ReadOnlySpan<byte> sender,
        ReadOnlySpan<byte> recipient,
        ulong amountAtomic,
        ulong feeAtomic,
        ulong nonce)
    {
        var buffer = new byte[4 + 32 + 32 + 8 + 8 + 8];
        var offset = 0;

        BinaryPrimitives.WriteUInt32BigEndian(buffer.AsSpan(offset, 4), chainId);
        offset += 4;
        sender.CopyTo(buffer.AsSpan(offset, 32));
        offset += 32;
        recipient.CopyTo(buffer.AsSpan(offset, 32));
        offset += 32;
        BinaryPrimitives.WriteUInt64BigEndian(buffer.AsSpan(offset, 8), amountAtomic);
        offset += 8;
        BinaryPrimitives.WriteUInt64BigEndian(buffer.AsSpan(offset, 8), feeAtomic);
        offset += 8;
        BinaryPrimitives.WriteUInt64BigEndian(buffer.AsSpan(offset, 8), nonce);

        return buffer;
    }

    private static byte[] BuildRawTransactionBytes(
        uint chainId,
        ReadOnlySpan<byte> sender,
        ReadOnlySpan<byte> recipient,
        ulong amountAtomic,
        ulong feeAtomic,
        ulong nonce,
        ReadOnlySpan<byte> signature)
    {
        var buffer = new byte[1 + 4 + 32 + 32 + 16 + 16 + 8 + 2 + signature.Length];
        var offset = 0;

        buffer[offset++] = FormatVersion;
        BinaryPrimitives.WriteUInt32BigEndian(buffer.AsSpan(offset, 4), chainId);
        offset += 4;
        sender.CopyTo(buffer.AsSpan(offset, 32));
        offset += 32;
        recipient.CopyTo(buffer.AsSpan(offset, 32));
        offset += 32;
        WriteU64AsU128BigEndian(buffer.AsSpan(offset, 16), amountAtomic);
        offset += 16;
        WriteU64AsU128BigEndian(buffer.AsSpan(offset, 16), feeAtomic);
        offset += 16;
        BinaryPrimitives.WriteUInt64BigEndian(buffer.AsSpan(offset, 8), nonce);
        offset += 8;
        BinaryPrimitives.WriteUInt16BigEndian(buffer.AsSpan(offset, 2), checked((ushort)signature.Length));
        offset += 2;
        signature.CopyTo(buffer.AsSpan(offset, signature.Length));

        return buffer;
    }

    private static void WriteU64AsU128BigEndian(Span<byte> destination, ulong value)
    {
        BinaryPrimitives.WriteUInt64BigEndian(destination[..8], 0UL);
        BinaryPrimitives.WriteUInt64BigEndian(destination[8..], value);
    }
}
