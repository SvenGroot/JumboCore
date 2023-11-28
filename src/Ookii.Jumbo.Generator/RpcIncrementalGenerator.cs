using System.Collections.Immutable;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

namespace Ookii.Jumbo.Generator;

[Generator]
public class RpcIncrementalGenerator : IIncrementalGenerator
{
    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var interfaceDeclarations = context.SyntaxProvider
            .CreateSyntaxProvider(
                static (node, _) => node is InterfaceDeclarationSyntax c && c.AttributeLists.Count > 0,
                static (ctx, _) => GetClassToGenerate(ctx)
            )
            .Where(static c => c != null);

        var compilationAndClasses = context.CompilationProvider.Combine(interfaceDeclarations.Collect());
        context.RegisterSourceOutput(compilationAndClasses, static (spc, source) => Execute(source.Left, source.Right!, spc));
    }

    private static void Execute(Compilation compilation, ImmutableArray<InterfaceDeclarationSyntax> interfaces, SourceProductionContext context)
    {
        if (interfaces.IsDefaultOrEmpty)
        {
            return;
        }

        var typeHelper = new TypeHelper(compilation);
        foreach (var cls in interfaces)
        {
            context.CancellationToken.ThrowIfCancellationRequested();
            var semanticModel = compilation.GetSemanticModel(cls.SyntaxTree);
            if (semanticModel.GetDeclaredSymbol(cls, context.CancellationToken) is not INamedTypeSymbol symbol)
            {
                continue;
            }

            var source = RpcGenerator.Generate(context, symbol, typeHelper);
            if (source != null)
            {
                context.AddSource(symbol.ToDisplayString().ToIdentifier(".g.cs"), SourceText.From(source, Encoding.UTF8));
            }
        }
    }

    private static InterfaceDeclarationSyntax? GetClassToGenerate(GeneratorSyntaxContext context)
    {
        var classDeclaration = (InterfaceDeclarationSyntax)context.Node;
        var typeHelper = new TypeHelper(context.SemanticModel.Compilation);
        var rpcInterfaceType = typeHelper.RpcInterfaceAttribute;
        foreach (var attributeList in classDeclaration.AttributeLists)
        {
            foreach (var attribute in attributeList.Attributes)
            {
                if (context.SemanticModel.GetSymbolInfo(attribute).Symbol is not IMethodSymbol attributeSymbol)
                {
                    // No symbol for the attribute for some reason.
                    continue;
                }

                var attributeType = attributeSymbol.ContainingType;
                if (attributeType.SymbolEquals(rpcInterfaceType))
                {
                    return classDeclaration;
                }
            }
        }

        return null;
    }
}
