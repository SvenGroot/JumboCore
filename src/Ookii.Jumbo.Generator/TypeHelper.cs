using Microsoft.CodeAnalysis;
using System.ComponentModel;
using System.Globalization;
using System.Reflection;

namespace Ookii.Jumbo.Generator;

internal class TypeHelper
{
    private readonly Compilation _compilation;
    private const string IoNamespacePrefix = "Ookii.Jumbo.IO.";
    private const string RpcNamespacePrefix = "Ookii.Jumbo.Rpc.";

    public TypeHelper(Compilation compilation)
    {
        _compilation = compilation;
    }

    public Compilation Compilation => _compilation;

    public INamedTypeSymbol Boolean => _compilation.GetSpecialType(SpecialType.System_Boolean);

    public INamedTypeSymbol Char => _compilation.GetSpecialType(SpecialType.System_Char);

    public INamedTypeSymbol? Dictionary => _compilation.GetTypeByMetadataName(typeof(Dictionary<,>).FullName);

    public INamedTypeSymbol? IDictionary => _compilation.GetTypeByMetadataName(typeof(IDictionary<,>).FullName);

    public INamedTypeSymbol? ICollection => _compilation.GetTypeByMetadataName(typeof(ICollection<>).FullName);

    public INamedTypeSymbol? DescriptionAttribute => _compilation.GetTypeByMetadataName(typeof(DescriptionAttribute).FullName);

    public INamedTypeSymbol? AssemblyDescriptionAttribute => _compilation.GetTypeByMetadataName(typeof(AssemblyDescriptionAttribute).FullName);

    public INamedTypeSymbol? TypeConverterAttribute => _compilation.GetTypeByMetadataName(typeof(TypeConverterAttribute).FullName);

    public INamedTypeSymbol? IWritable => _compilation.GetTypeByMetadataName(IoNamespacePrefix + "IWritable");

    public INamedTypeSymbol? GeneratedWritableAttribute => _compilation.GetTypeByMetadataName(IoNamespacePrefix + "GeneratedWritableAttribute");

    public INamedTypeSymbol? GeneratedValueWriterAttribute => _compilation.GetTypeByMetadataName(IoNamespacePrefix + "GeneratedValueWriterAttribute");

    public INamedTypeSymbol? WritableNotNullAttribute => _compilation.GetTypeByMetadataName(IoNamespacePrefix + "WritableNotNullAttribute");

    public INamedTypeSymbol? WritableIgnoreAttribute => _compilation.GetTypeByMetadataName(IoNamespacePrefix + "WritableIgnoreAttribute");

    public INamedTypeSymbol? RpcInterfaceAttribute => _compilation.GetTypeByMetadataName(RpcNamespacePrefix + "RpcInterfaceAttribute");
}
