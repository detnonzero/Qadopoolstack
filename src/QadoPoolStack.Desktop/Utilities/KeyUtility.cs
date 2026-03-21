using System.Text;
using NSec.Cryptography;

namespace QadoPoolStack.Desktop.Utilities;

public static class KeyUtility
{
    private static readonly SignatureAlgorithm Algorithm = SignatureAlgorithm.Ed25519;

    public static (string privateKeyHex, string publicKeyHex) GenerateEd25519KeypairHex()
    {
        using var key = Key.Create(Algorithm, new KeyCreationParameters
        {
            ExportPolicy = KeyExportPolicies.AllowPlaintextExport
        });

        var privateKey = key.Export(KeyBlobFormat.RawPrivateKey);
        var publicKey = key.Export(KeyBlobFormat.RawPublicKey);
        return (HexUtility.ToLowerHex(privateKey), HexUtility.ToLowerHex(publicKey));
    }

    public static string DeriveEd25519PublicKeyHex(string privateKeyHex)
    {
        var privateKeyBytes = HexUtility.Parse(privateKeyHex, 32);
        using var key = Key.Import(Algorithm, privateKeyBytes, KeyBlobFormat.RawPrivateKey, new KeyCreationParameters
        {
            ExportPolicy = KeyExportPolicies.AllowPlaintextExport
        });
        var publicKey = key.Export(KeyBlobFormat.RawPublicKey);
        return HexUtility.ToLowerHex(publicKey);
    }

    public static byte[] SignEd25519(string privateKeyHex, byte[] message)
    {
        ArgumentNullException.ThrowIfNull(message);

        var privateKeyBytes = HexUtility.Parse(privateKeyHex, 32);
        using var key = Key.Import(Algorithm, privateKeyBytes, KeyBlobFormat.RawPrivateKey, new KeyCreationParameters
        {
            ExportPolicy = KeyExportPolicies.AllowPlaintextExport
        });
        return Algorithm.Sign(key, message);
    }

    public static bool VerifyEd25519Signature(string publicKeyHex, string message, string signatureHex)
    {
        var publicKeyBytes = HexUtility.Parse(publicKeyHex, 32);
        var signatureBytes = HexUtility.Parse(signatureHex, 64);
        var payload = Encoding.UTF8.GetBytes(message);
        var publicKey = PublicKey.Import(Algorithm, publicKeyBytes, KeyBlobFormat.RawPublicKey);
        return Algorithm.Verify(publicKey, payload, signatureBytes);
    }
}
