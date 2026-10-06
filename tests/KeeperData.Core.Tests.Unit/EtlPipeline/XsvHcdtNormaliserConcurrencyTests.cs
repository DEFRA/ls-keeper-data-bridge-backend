using System.Text;
using FluentAssertions;
using KeeperData.Core.EtlPipeline.Setup;
using Microsoft.Extensions.DependencyInjection;
using XsvHcdtHelper;

namespace KeeperData.Core.Tests.Unit.EtlPipeline;

/// <summary>The normalise stage is about to drive one shared <see cref="IXsvHcdtNormaliser"/> from
/// several threads at once. That instance holds a base options field and every call hands it a
/// lambda that mutates an options object, so whether the object is per-call or shared decides
/// whether fanning the stage out silently corrupts output. These tests pin the answer down against
/// the registration the pipeline actually uses, so a package upgrade that changes it fails here
/// rather than in a run.</summary>
[Trait("Category", "Unit")]
public class XsvHcdtNormaliserConcurrencyTests
{
    /// <summary>Parquet's magic number: the first four bytes of any Parquet file.</summary>
    private static readonly byte[] ParquetMagic = "PAR1"u8.ToArray();

    [Fact]
    public async Task Each_call_configures_its_own_options_instance()
    {
        var normaliser = Normaliser();
        var seen = new List<XsvHcdtOptions>();

        for (var i = 0; i < 4; i++)
        {
            await normaliser.NormaliseAsync(
                Hcdt(dataRecords: 1), new MemoryStream(), options => seen.Add(options), CancellationToken.None);
        }

        // Reference, not value, equality: a shared instance is the thing being ruled out.
        for (var i = 0; i < seen.Count; i++)
        {
            for (var j = i + 1; j < seen.Count; j++)
            {
                ReferenceEquals(seen[i], seen[j]).Should().BeFalse(
                    "calls {0} and {1} must not configure the same options object, or a concurrent "
                    + "call would overwrite another's settings mid-conversion", i, j);
            }
        }
    }

    [Fact]
    public async Task Concurrent_calls_do_not_leak_options_or_parser_state()
    {
        var normaliser = Normaliser();

        const int iterations = 200;
        var produced = new byte[iterations][];
        var declared = new long[iterations];

        await Parallel.ForAsync(0, iterations, async (i, cancellationToken) =>
        {
            // Alternating the output format pits each call's options against the registered default,
            // and varying the record count does the same for whatever state the parser carries.
            var wantsParquet = i % 2 == 0;
            var dataRecords = (i % 3) + 1;

            var output = new MemoryStream();

            var report = await normaliser.NormaliseAsync(
                Hcdt(dataRecords),
                output,
                options => options.OutputFormat = wantsParquet ? OutputFormat.Parquet : OutputFormat.Csv,
                cancellationToken);

            produced[i] = output.ToArray();
            declared[i] = report.ActualDataRecords;
        });

        for (var i = 0; i < iterations; i++)
        {
            IsParquet(produced[i]).Should().Be(i % 2 == 0,
                "call {0} asked for {1} and must get it regardless of what ran alongside it",
                i, i % 2 == 0 ? "Parquet" : "Csv");

            declared[i].Should().Be((i % 3) + 1,
                "call {0} fed in {1} data record(s) and must be reported on its own input", i, (i % 3) + 1);
        }
    }

    private static IXsvHcdtNormaliser Normaliser()
        => new ServiceCollection()
            .AddEtlPipeline()
            .BuildServiceProvider()
            .GetRequiredService<IXsvHcdtNormaliser>();

    private static bool IsParquet(byte[] content)
        => content.Length >= ParquetMagic.Length && content.AsSpan(0, ParquetMagic.Length).SequenceEqual(ParquetMagic);

    /// <summary>A minimal valid H/C/D/T document carrying the requested number of data records.</summary>
    private static MemoryStream Hcdt(int dataRecords)
    {
        const string stamp = "01012026 07:28:26";
        const string fileName = "LITP_SAMCPHHOLDING_20260101.csv";

        var builder = new StringBuilder()
            .Append("H|").Append(fileName).Append('|').Append(stamp).Append("\r\n")
            .Append("C|RECORD_TYPE|RECORD_COUNT|CPH|DISEASE_TYPE|CHANGETYPE\r\n");

        for (var record = 1; record <= dataRecords; record++)
        {
            builder.Append("D|").Append(record).Append("|12/345/678").Append(record).Append("|TB|I\r\n");
        }

        builder.Append("T|").Append(fileName).Append('|').Append(stamp).Append('|').Append(dataRecords).Append("\r\n");

        return new MemoryStream(Encoding.UTF8.GetBytes(builder.ToString()));
    }
}
