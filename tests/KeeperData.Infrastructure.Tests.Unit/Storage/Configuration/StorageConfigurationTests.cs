using FluentAssertions;
using KeeperData.Infrastructure.Storage.Configuration;

namespace KeeperData.Infrastructure.Tests.Unit.Storage.Configuration;

public class StorageConfigurationTests
{
    [Fact]
    public void DefaultValues_AreSetCorrectly()
    {
        var config = new StorageConfiguration
        {
            SourceExternalPrefix = "external",
            SourceInternalPrefix = "internal",
            TargetInternalPrefix = "target"
        };

        config.ExternalStorage.Should().NotBeNull();
        config.InternalStorage.Should().NotBeNull();
        config.SourceExternalPrefix.Should().Be("external");
        config.SourceInternalPrefix.Should().Be("internal");
        config.TargetInternalPrefix.Should().Be("target");
    }

    [Fact]
    public void ExternalStorage_CanBeConfigured()
    {
        var config = new StorageConfiguration
        {
            SourceExternalPrefix = "ext",
            SourceInternalPrefix = "int",
            TargetInternalPrefix = "tgt",
            ExternalStorage = new StorageWithCredentialsConfiguration
            {
                BucketName = "external-bucket",
                HealthcheckEnabled = true,
                AccessKeySecretName = "access-key",
                SecretKeySecretName = "secret-key"
            }
        };

        config.ExternalStorage.BucketName.Should().Be("external-bucket");
        config.ExternalStorage.HealthcheckEnabled.Should().BeTrue();
        config.ExternalStorage.AccessKeySecretName.Should().Be("access-key");
        config.ExternalStorage.SecretKeySecretName.Should().Be("secret-key");
    }

    [Fact]
    public void InternalStorage_CanBeConfigured()
    {
        var config = new StorageConfiguration
        {
            SourceExternalPrefix = "ext",
            SourceInternalPrefix = "int",
            TargetInternalPrefix = "tgt",
            InternalStorage = new StorageConfigurationDetails
            {
                BucketName = "internal-bucket",
                HealthcheckEnabled = true
            }
        };

        config.InternalStorage.BucketName.Should().Be("internal-bucket");
        config.InternalStorage.HealthcheckEnabled.Should().BeTrue();
    }
}

public class StorageConfigurationDetailsTests
{
    [Fact]
    public void DefaultValues_AreSetCorrectly()
    {
        var config = new StorageConfigurationDetails();

        config.HealthcheckEnabled.Should().BeFalse();
        config.BucketName.Should().BeEmpty();
    }

    [Fact]
    public void CustomValues_CanBeSet()
    {
        var config = new StorageConfigurationDetails
        {
            HealthcheckEnabled = true,
            BucketName = "my-bucket"
        };

        config.HealthcheckEnabled.Should().BeTrue();
        config.BucketName.Should().Be("my-bucket");
    }

    [Fact]
    public void Record_SupportsEquality()
    {
        var config1 = new StorageConfigurationDetails { BucketName = "bucket1" };
        var config2 = new StorageConfigurationDetails { BucketName = "bucket1" };
        var config3 = new StorageConfigurationDetails { BucketName = "bucket2" };

        config1.Should().Be(config2);
        config1.Should().NotBe(config3);
    }
}

public class StorageWithCredentialsConfigurationTests
{
    [Fact]
    public void DefaultValues_AreSetCorrectly()
    {
        var config = new StorageWithCredentialsConfiguration();

        config.HealthcheckEnabled.Should().BeFalse();
        config.BucketName.Should().BeEmpty();
        config.AccessKeySecretName.Should().BeEmpty();
        config.SecretKeySecretName.Should().BeEmpty();
    }

    [Fact]
    public void CustomValues_CanBeSet()
    {
        var config = new StorageWithCredentialsConfiguration
        {
            HealthcheckEnabled = true,
            BucketName = "my-bucket",
            AccessKeySecretName = "my-access-key",
            SecretKeySecretName = "my-secret-key"
        };

        config.HealthcheckEnabled.Should().BeTrue();
        config.BucketName.Should().Be("my-bucket");
        config.AccessKeySecretName.Should().Be("my-access-key");
        config.SecretKeySecretName.Should().Be("my-secret-key");
    }

    [Fact]
    public void InheritsFromStorageConfigurationDetails()
    {
        var config = new StorageWithCredentialsConfiguration();

        config.Should().BeAssignableTo<StorageConfigurationDetails>();
    }

    [Fact]
    public void Record_SupportsWithExpression()
    {
        var original = new StorageWithCredentialsConfiguration { BucketName = "original" };
        var modified = original with { BucketName = "modified", AccessKeySecretName = "key" };

        modified.BucketName.Should().Be("modified");
        modified.AccessKeySecretName.Should().Be("key");
        original.BucketName.Should().Be("original");
    }
}
