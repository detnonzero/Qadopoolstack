using System.Globalization;
using System.Numerics;
using System.Security.Cryptography;
using System.Text;

namespace QadoPoolStack.Desktop.Utilities;

public static class HexUtility
{
    public static bool IsHex(string? value, int byteLength)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        value = value.Trim();
        if (value.Length != byteLength * 2)
        {
            return false;
        }

        for (var i = 0; i < value.Length; i++)
        {
            var c = value[i];
            var valid =
                (c >= '0' && c <= '9') ||
                (c >= 'a' && c <= 'f') ||
                (c >= 'A' && c <= 'F');

            if (!valid)
            {
                return false;
            }
        }

        return true;
    }

    public static string NormalizeLower(string value, int byteLength)
    {
        if (!IsHex(value, byteLength))
        {
            throw new FormatException($"Expected {byteLength} bytes of hex.");
        }

        return value.Trim().ToLowerInvariant();
    }

    public static byte[] Parse(string value, int byteLength)
    {
        return Convert.FromHexString(NormalizeLower(value, byteLength));
    }

    public static string ToLowerHex(byte[] value) => Convert.ToHexString(value).ToLowerInvariant();

    public static string CreateTokenHex(int byteLength)
    {
        var bytes = RandomNumberGenerator.GetBytes(byteLength);
        return ToLowerHex(bytes);
    }

    public static byte[] HashSha256(string value)
    {
        return SHA256.HashData(Encoding.UTF8.GetBytes(value));
    }

    public static string HashSha256Hex(string value) => ToLowerHex(HashSha256(value));
}

public static class UInt256Utility
{
    private static readonly BigInteger MaxValue = (BigInteger.One << 256) - BigInteger.One;

    public static string ComputeShareTargetHex(double difficulty, string networkTargetHex)
    {
        if (difficulty <= 0)
        {
            difficulty = 1d;
        }

        var networkTarget = ParseHex(networkTargetHex);
        var computed = MaxValue / new BigInteger(Math.Max(1d, difficulty));
        if (computed < networkTarget)
        {
            computed = networkTarget;
        }

        if (computed > MaxValue)
        {
            computed = MaxValue;
        }

        return ToFixedHex(computed);
    }

    public static bool IsHashAtOrBelowTarget(byte[] hash, string targetHex)
    {
        return Compare(hash, ParseHex(targetHex)) <= 0;
    }

    public static int Compare(byte[] hash, BigInteger target)
    {
        var hashValue = new BigInteger(hash, isUnsigned: true, isBigEndian: true);
        return hashValue.CompareTo(target);
    }

    public static BigInteger ParseHex(string value)
    {
        var normalized = HexUtility.NormalizeLower(value, 32);
        return new BigInteger(Convert.FromHexString(normalized), isUnsigned: true, isBigEndian: true);
    }

    public static string ToFixedHex(BigInteger value)
    {
        var bytes = value.ToByteArray(isUnsigned: true, isBigEndian: true);
        if (bytes.Length > 32)
        {
            throw new InvalidOperationException("UInt256 overflow.");
        }

        if (bytes.Length == 32)
        {
            return Convert.ToHexString(bytes).ToLowerInvariant();
        }

        var padded = new byte[32];
        bytes.CopyTo(padded.AsSpan(32 - bytes.Length));
        return Convert.ToHexString(padded).ToLowerInvariant();
    }
}

public static class AmountUtility
{
    private const int Decimals = 9;

    public static string FormatAtomic(long atomic)
    {
        var sign = atomic < 0 ? "-" : "";
        var absolute = Math.Abs(atomic);
        var major = absolute / 1_000_000_000L;
        var minor = absolute % 1_000_000_000L;
        return $"{sign}{major}.{minor.ToString("D9", CultureInfo.InvariantCulture).TrimEnd('0').PadRight(1, '0')}";
    }

    public static bool TryParseToAtomic(string? value, out long atomic)
    {
        atomic = 0;
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        var normalized = value.Trim().Replace(" ", string.Empty, StringComparison.Ordinal);
        if (normalized.Contains(',', StringComparison.Ordinal) && !normalized.Contains('.', StringComparison.Ordinal))
        {
            normalized = normalized.Replace(',', '.');
        }

        if (!decimal.TryParse(normalized, NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture, out var parsed))
        {
            return false;
        }

        parsed *= (decimal)Math.Pow(10, Decimals);
        parsed = decimal.Truncate(parsed);

        if (parsed < 0 || parsed > long.MaxValue)
        {
            return false;
        }

        atomic = (long)parsed;
        return true;
    }
}
