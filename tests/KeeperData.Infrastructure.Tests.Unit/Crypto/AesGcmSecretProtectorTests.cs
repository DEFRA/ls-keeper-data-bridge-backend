using FluentAssertions;
using KeeperData.Infrastructure.Crypto;
using System.Security.Cryptography;

namespace KeeperData.Infrastructure.Tests.Unit.Crypto;

public class AesGcmSecretProtectorTests
{
    private const string EnvVarName = "AESGCM_PROTECTOR_TESTS_KEY";
    private static readonly byte[] TestKey = new byte[32];

    static AesGcmSecretProtectorTests()
    {
        // Deterministic non-zero test key.
        for (var i = 0; i < TestKey.Length; i++) TestKey[i] = (byte)(i + 1);
    }

    [Fact]
    public void ProtectThenUnprotect_RoundTripsValue()
    {
        // Arrange
        var sut = AesGcmSecretProtector.FromKey(TestKey);

        // Act
        var encrypted = sut.Protect("wJalrXUtnFEMIK7MDENG", "purpose-a");
        var decrypted = sut.Unprotect(encrypted, "purpose-a");

        // Assert
        decrypted.Should().Be("wJalrXUtnFEMIK7MDENG");
        encrypted.KeyVersion.Should().Be(AesGcmSecretProtector.CurrentKeyVersion);
    }

    [Fact]
    public void Protect_ProducesUniqueNoncePerCall()
    {
        // Arrange
        var sut = AesGcmSecretProtector.FromKey(TestKey);

        // Act
        var first = sut.Protect("same-value", "p");
        var second = sut.Protect("same-value", "p");

        // Assert
        first.Nonce.Should().NotBe(second.Nonce);
        first.CipherText.Should().NotBe(second.CipherText);
    }

    [Fact]
    public void Unprotect_WithWrongPurpose_Throws()
    {
        // Arrange
        var sut = AesGcmSecretProtector.FromKey(TestKey);
        var encrypted = sut.Protect("value", "access-key-id");

        // Act
        var act = () => sut.Unprotect(encrypted, "secret-access-key");

        // Assert
        act.Should().Throw<CryptographicException>();
    }

    [Fact]
    public void Unprotect_WithTamperedCipherText_Throws()
    {
        // Arrange
        var sut = AesGcmSecretProtector.FromKey(TestKey);
        var encrypted = sut.Protect("value", "p");
        var tamperedBytes = Convert.FromBase64String(encrypted.CipherText);
        tamperedBytes[0] ^= 0xFF;
        var tampered = encrypted with { CipherText = Convert.ToBase64String(tamperedBytes) };

        // Act
        var act = () => sut.Unprotect(tampered, "p");

        // Assert
        act.Should().Throw<CryptographicException>();
    }

    [Fact]
    public void Unprotect_WithWrongKey_Throws()
    {
        // Arrange
        var encrypted = AesGcmSecretProtector.FromKey(TestKey).Protect("value", "p");
        var otherKey = new byte[32];
        otherKey[0] = 0xAA;
        var sut = AesGcmSecretProtector.FromKey(otherKey);

        // Act
        var act = () => sut.Unprotect(encrypted, "p");

        // Assert
        act.Should().Throw<CryptographicException>();
    }

    [Fact]
    public void FromKey_WithWrongKeyLength_Throws()
    {
        // Act
        var act = () => AesGcmSecretProtector.FromKey(new byte[16]);

        // Assert
        act.Should().Throw<InvalidOperationException>().WithMessage("*32 bytes*16 bytes*");
    }

    [Fact]
    public void FromEnvironment_WithMissingVariable_ReturnsDormantProtector()
    {
        // Arrange
        Environment.SetEnvironmentVariable(EnvVarName, null);

        // Act
        var sut = AesGcmSecretProtector.FromEnvironment(EnvVarName);

        // Assert
        sut.IsConfigured.Should().BeFalse();
    }

    [Fact]
    public void FromEnvironment_WithInvalidBase64_FailsFast()
    {
        // Arrange
        Environment.SetEnvironmentVariable(EnvVarName, "not base64 !!!");

        try
        {
            // Act
            var act = () => AesGcmSecretProtector.FromEnvironment(EnvVarName);

            // Assert
            act.Should().Throw<InvalidOperationException>().WithMessage("*not valid base64*");
        }
        finally
        {
            Environment.SetEnvironmentVariable(EnvVarName, null);
        }
    }

    [Fact]
    public void FromEnvironment_WithWrongKeySize_FailsFast()
    {
        // Arrange
        Environment.SetEnvironmentVariable(EnvVarName, Convert.ToBase64String(new byte[16]));

        try
        {
            // Act
            var act = () => AesGcmSecretProtector.FromEnvironment(EnvVarName);

            // Assert
            act.Should().Throw<InvalidOperationException>().WithMessage("*32 bytes*");
        }
        finally
        {
            Environment.SetEnvironmentVariable(EnvVarName, null);
        }
    }

    [Fact]
    public void FromEnvironment_WithValidKey_IsConfigured()
    {
        // Arrange
        Environment.SetEnvironmentVariable(EnvVarName, Convert.ToBase64String(TestKey));

        try
        {
            // Act
            var sut = AesGcmSecretProtector.FromEnvironment(EnvVarName);

            // Assert
            sut.IsConfigured.Should().BeTrue();
        }
        finally
        {
            Environment.SetEnvironmentVariable(EnvVarName, null);
        }
    }

    [Fact]
    public void Protect_WhenNotConfigured_Throws()
    {
        // Arrange
        var sut = AesGcmSecretProtector.Unconfigured();

        // Act
        var act = () => sut.Protect("value", "p");

        // Assert
        act.Should().Throw<InvalidOperationException>().WithMessage("*not configured*");
    }

    [Fact]
    public void Unprotect_WhenNotConfigured_Throws()
    {
        // Arrange
        var encrypted = AesGcmSecretProtector.FromKey(TestKey).Protect("value", "p");
        var sut = AesGcmSecretProtector.Unconfigured();

        // Act
        var act = () => sut.Unprotect(encrypted, "p");

        // Assert
        act.Should().Throw<InvalidOperationException>().WithMessage("*not configured*");
    }

    [Fact]
    public void Unprotect_WithMismatchedKeyVersion_Throws()
    {
        // Arrange
        var sut = AesGcmSecretProtector.FromKey(TestKey);
        var encrypted = sut.Protect("value", "p") with { KeyVersion = 99 };

        // Act
        var act = () => sut.Unprotect(encrypted, "p");

        // Assert
        act.Should().Throw<InvalidOperationException>().WithMessage("*key version 99*");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("  ")]
    public void FromEnvironment_WithBlankSecretName_Throws(string? secretName)
    {
        // Act
        var act = () => AesGcmSecretProtector.FromEnvironment(secretName!);

        // Assert
        act.Should().Throw<ArgumentException>();
    }
}
