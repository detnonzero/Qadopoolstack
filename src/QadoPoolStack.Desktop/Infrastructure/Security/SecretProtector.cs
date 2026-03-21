using System.Security.Cryptography;
using System.Text;

namespace QadoPoolStack.Desktop.Infrastructure.Security;

public sealed class SecretProtector
{
    public string Protect(string plaintext)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(plaintext);

        var data = Encoding.UTF8.GetBytes(plaintext);
        var protectedData = ProtectedData.Protect(data, null, DataProtectionScope.CurrentUser);
        return Convert.ToBase64String(protectedData);
    }

    public string Unprotect(string protectedBase64)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(protectedBase64);

        var data = Convert.FromBase64String(protectedBase64);
        var plaintext = ProtectedData.Unprotect(data, null, DataProtectionScope.CurrentUser);
        return Encoding.UTF8.GetString(plaintext);
    }

    public string? TryUnprotect(string? protectedBase64)
    {
        if (string.IsNullOrWhiteSpace(protectedBase64))
        {
            return null;
        }

        try
        {
            return Unprotect(protectedBase64);
        }
        catch
        {
            return null;
        }
    }
}
