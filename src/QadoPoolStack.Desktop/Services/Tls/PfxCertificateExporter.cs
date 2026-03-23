using System.Reflection;
using System.Security.Cryptography.X509Certificates;
using Certes;
using Certes.Acme;

namespace QadoPoolStack.Desktop.Services.Tls;

internal static class PfxCertificateExporter
{
    public static byte[] Build(CertificateChain certificateChain, IKey certificateKey, string password)
    {
        ArgumentNullException.ThrowIfNull(certificateChain);
        ArgumentNullException.ThrowIfNull(certificateKey);

        if (string.IsNullOrWhiteSpace(password))
        {
            throw new InvalidOperationException("Certificate password is required.");
        }

        var privateKeyPem = ExportPrivateKeyPem(certificateKey);
        using var leafCertificate = X509Certificate2.CreateFromPem(certificateChain.Certificate.ToPem(), privateKeyPem);
        var issuerCertificates = new List<X509Certificate2>();

        try
        {
            var exportCollection = new X509Certificate2Collection
            {
                leafCertificate
            };

            foreach (var issuer in certificateChain.Issuers)
            {
                var issuerCertificate = X509CertificateLoader.LoadCertificate(issuer.ToDer());
                issuerCertificates.Add(issuerCertificate);
                exportCollection.Add(issuerCertificate);
            }

            return exportCollection.Export(X509ContentType.Pfx, password)
                ?? throw new InvalidOperationException("Unable to export certificate archive.");
        }
        finally
        {
            foreach (var issuerCertificate in issuerCertificates)
            {
                issuerCertificate.Dispose();
            }
        }
    }

    private static string ExportPrivateKeyPem(IKey certificateKey)
    {
        var toPem = certificateKey.GetType().GetMethod(
            "ToPem",
            BindingFlags.Instance | BindingFlags.Public,
            binder: null,
            types: [],
            modifiers: null);

        if (toPem?.Invoke(certificateKey, null) is string pem && !string.IsNullOrWhiteSpace(pem))
        {
            return pem;
        }

        throw new InvalidOperationException("Unable to export certificate private key.");
    }
}
