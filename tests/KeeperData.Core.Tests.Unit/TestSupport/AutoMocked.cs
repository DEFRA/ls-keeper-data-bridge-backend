using Microsoft.Extensions.Options;
using Moq;

namespace KeeperData.Core.Tests.Unit.TestSupport;

/// <summary>Builds a real instance of a type with every constructor dependency supplied as a
/// default Moq mock. For tests that only need a valid instance - pipeline wiring and lineup
/// checks, for example - and don't care how the dependencies behave.
///
/// When a test needs to drive or assert on a dependency, construct the type explicitly with
/// the mocks it wants to keep a handle on. This helper is for the throwaway case.
///
/// Dependencies must be interfaces or non-sealed classes (which covers stage dependencies:
/// service interfaces and ILogger&lt;T&gt;). A sealed dependency can't be mocked and will fail.</summary>
public static class AutoMocked
{
    public static T Instance<T>() where T : class
    {
        var constructor = typeof(T).GetConstructors()
            .OrderByDescending(c => c.GetParameters().Length)
            .First();

        var arguments = constructor.GetParameters()
            .Select(p => CreateArgument(p.ParameterType))
            .ToArray();

        return (T)constructor.Invoke(arguments);
    }

    private static object CreateArgument(Type type)
    {
        // A mocked IOptions<T> hands back a null Value, which a constructor reading its options
        // dereferences. Real options carrying defaults are what such a type would get in a host.
        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IOptions<>))
        {
            var optionsType = type.GetGenericArguments()[0];

            return typeof(Options)
                .GetMethod(nameof(Options.Create))!
                .MakeGenericMethod(optionsType)
                .Invoke(null, [Activator.CreateInstance(optionsType)])!;
        }

        if (type.IsInterface || (type.IsClass && !type.IsSealed))
        {
            var mock = (Mock)Activator.CreateInstance(typeof(Mock<>).MakeGenericType(type))!;
            return mock.Object;
        }

        if (type.IsValueType)
        {
            return Activator.CreateInstance(type)!;
        }

        throw new InvalidOperationException(
            $"{type.Name} is sealed, so it cannot be mocked. Construct the type under test explicitly "
            + "and hand it a real one.");
    }
}
