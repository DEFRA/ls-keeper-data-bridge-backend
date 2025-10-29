using Bogus;
using System.Text;

namespace KeeperData.Bridge.Tests.Integration.Helpers;

public static class TestDataGenerator
{
    public static (string CsvContent, List<PersonRecord> Records) GeneratePersonCsv(int recordCount, string primaryKeyHeaderName = "PersonId")
    {
        var faker = new Faker<PersonRecord>()
            .RuleFor(p => p.PersonId, f => f.Random.Guid().ToString())
            .RuleFor(p => p.FirstName, f => f.Name.FirstName())
            .RuleFor(p => p.LastName, f => f.Name.LastName())
            .RuleFor(p => p.Email, (f, p) => f.Internet.Email(p.FirstName, p.LastName))
            .RuleFor(p => p.PhoneNumber, f => f.Phone.PhoneNumber("###-###-####"))
            .RuleFor(p => p.DateOfBirth, f => f.Date.Past(50, DateTime.Now.AddYears(-18)).ToString("yyyy-MM-dd"))
            .RuleFor(p => p.Address, f => f.Address.FullAddress())
            .RuleFor(p => p.City, f => f.Address.City())
            .RuleFor(p => p.PostalCode, f => f.Address.ZipCode())
            .RuleFor(p => p.Country, f => f.Address.Country())
            .RuleFor(p => p.Salary, f => f.Random.Int(25000, 150000))
            .RuleFor(p => p.Department, f => f.PickRandom("Engineering", "Sales", "Marketing", "HR", "Finance", "Operations"))
            .RuleFor(p => p.IsActive, f => f.Random.Bool(0.9f))
            .RuleFor(p => p.ChangeType, _ => "I");

        var records = faker.Generate(recordCount);

        var csv = new StringBuilder();
        csv.AppendLine($"{primaryKeyHeaderName},FirstName,LastName,Email,PhoneNumber,DateOfBirth,Address,City,PostalCode,Country,Salary,Department,IsActive,CHANGETYPE");

        foreach (var record in records)
        {
            csv.AppendLine($"\"{record.PersonId}\",\"{record.FirstName}\",\"{record.LastName}\",\"{record.Email}\",\"{record.PhoneNumber}\",\"{record.DateOfBirth}\",\"{EscapeCsv(record.Address)}\",\"{record.City}\",\"{record.PostalCode}\",\"{record.Country}\",{record.Salary},\"{record.Department}\",{record.IsActive.ToString().ToLower()},{record.ChangeType}");
        }

        return (csv.ToString(), records);
    }

    private static string EscapeCsv(string value)
    {
        return value.Replace("\"", "\"\"");
    }
}

public class PersonRecord
{
    public string PersonId { get; set; } = default!;
    public string FirstName { get; set; } = default!;
    public string LastName { get; set; } = default!;
    public string Email { get; set; } = default!;
    public string PhoneNumber { get; set; } = default!;
    public string DateOfBirth { get; set; } = default!;
    public string Address { get; set; } = default!;
    public string City { get; set; } = default!;
    public string PostalCode { get; set; } = default!;
    public string Country { get; set; } = default!;
    public int Salary { get; set; }
    public string Department { get; set; } = default!;
    public bool IsActive { get; set; }
    public string ChangeType { get; set; } = "I";
}