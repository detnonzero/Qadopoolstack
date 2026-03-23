using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Certes;
using Certes.Acme;
using QadoPoolStack.Desktop.Services.Tls;

namespace QadoPoolStack.Desktop.Tests;

public sealed class PfxCertificateExporterTests
{
    [Fact]
    public void Build_ExportsLeafAndIntermediateWhenRootIsMissing()
    {
        var notBefore = DateTimeOffset.UtcNow.AddDays(-1);
        var notAfter = DateTimeOffset.UtcNow.AddDays(30);

        using var rootKey = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var rootRequest = new CertificateRequest("CN=Test Root", rootKey, HashAlgorithmName.SHA256);
        rootRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 1, true));
        rootRequest.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign, true));
        rootRequest.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(rootRequest.PublicKey, false));

        using var rootCertificate = rootRequest.CreateSelfSigned(notBefore, notAfter);

        using var intermediateKey = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var intermediateRequest = new CertificateRequest("CN=Test Intermediate", intermediateKey, HashAlgorithmName.SHA256);
        intermediateRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        intermediateRequest.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign, true));
        intermediateRequest.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(intermediateRequest.PublicKey, false));

        using var intermediateCertificate = intermediateRequest.Create(rootCertificate, notBefore, notAfter, RandomNumberGenerator.GetBytes(16));
        using var intermediateCertificateWithKey = intermediateCertificate.CopyWithPrivateKey(intermediateKey);

        using var leafKey = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var leafRequest = new CertificateRequest("CN=pool.example.test", leafKey, HashAlgorithmName.SHA256);
        leafRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, true));
        leafRequest.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature, true));
        leafRequest.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(leafRequest.PublicKey, false));
        leafRequest.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(new OidCollection
        {
            new("1.3.6.1.5.5.7.3.1")
        }, critical: false));

        using var leafCertificate = leafRequest.Create(intermediateCertificateWithKey, notBefore, notAfter, RandomNumberGenerator.GetBytes(16));

        var certificateChain = new CertificateChain(string.Concat(
            leafCertificate.ExportCertificatePem(),
            intermediateCertificateWithKey.ExportCertificatePem()));

        var certificateKey = KeyFactory.FromPem(leafKey.ExportPkcs8PrivateKeyPem());
        var pfxBytes = PfxCertificateExporter.Build(certificateChain, certificateKey, "test-password");

        Assert.NotEmpty(pfxBytes);

        var importedCollection = X509CertificateLoader.LoadPkcs12Collection(
            pfxBytes,
            "test-password",
            X509KeyStorageFlags.Exportable | X509KeyStorageFlags.EphemeralKeySet,
            loaderLimits: null);

        try
        {
            Assert.Equal(2, importedCollection.Count);

            var importedLeaf = Assert.Single(importedCollection.Cast<X509Certificate2>(), certificate => certificate.HasPrivateKey);
            Assert.Equal("CN=pool.example.test", importedLeaf.Subject);
            Assert.DoesNotContain(importedCollection.Cast<X509Certificate2>(), certificate => certificate.Subject == "CN=Test Root");
        }
        finally
        {
            foreach (var certificate in importedCollection)
            {
                certificate.Dispose();
            }
        }
    }
}
