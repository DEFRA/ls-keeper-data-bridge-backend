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
        if (type.IsInterface || (type.IsClass && !type.IsSealed))
        {
            var mock = (Mock)Activator.CreateInstance(typeof(Mock<>).MakeGenericType(type))!;
            return mock.Object;
        }

        return type.IsValueType ? Activator.CreateInstance(type)! : null!;
    }
}
