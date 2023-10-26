using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;
using System.Collections.Immutable;
using System.Text;

namespace Ookii.Jumbo.Generator;

[Generator]
public class WritableIncrementalGenerator : IIncrementalGenerator
{
    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var classDeclarations = context.SyntaxProvider
            .CreateSyntaxProvider(
                static (node, _) => node is ClassDeclarationSyntax c && c.AttributeLists.Count > 0,
                static (ctx, _) => GetClassToGenerate(ctx)
            )
            .Where(static c => c != null);

        var compilationAndClasses = context.CompilationProvider.Combine(classDeclarations.Collect());
        context.RegisterSourceOutput(compilationAndClasses, static (spc, source) => Execute(source.Left, source.Right!, spc));
    }

    private static void Execute(Compilation compilation, ImmutableArray<ClassDeclarationSyntax> classes, SourceProductionContext context)
    {
        if (classes.IsDefaultOrEmpty)
        {
            return;
        }

        var typeHelper = new TypeHelper(compilation);
        foreach (var cls in classes)
        {
            context.CancellationToken.ThrowIfCancellationRequested();
            var semanticModel = compilation.GetSemanticModel(cls.SyntaxTree);
            if (semanticModel.GetDeclaredSymbol(cls, context.CancellationToken) is not INamedTypeSymbol symbol)
            {
                continue;
            }

            var source = WritableGenerator.Generate(context, symbol, typeHelper);
            if (source != null)
            {
                context.AddSource(symbol.ToDisplayString().ToIdentifier(".g.cs"), SourceText.From(source, Encoding.UTF8));
            }
        }
    }

    private static ClassDeclarationSyntax? GetClassToGenerate(GeneratorSyntaxContext context)
    {
        var classDeclaration = (ClassDeclarationSyntax)context.Node;
        var typeHelper = new TypeHelper(context.SemanticModel.Compilation);
        var generatedWritableType = typeHelper.GeneratedWritableAttribute;
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
                if (attributeType.SymbolEquals(generatedWritableType))
                {
                    return classDeclaration;
                }
            }
        }

        return null;
    }
}
