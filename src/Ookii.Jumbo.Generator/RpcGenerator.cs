using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis;

namespace Ookii.Jumbo.Generator;

internal class RpcGenerator
{
    private readonly TypeHelper _typeHelper;
    private readonly Compilation _compilation;
    private readonly SourceProductionContext _context;
    private readonly INamedTypeSymbol _target;
    private readonly SourceBuilder _builder;

    public RpcGenerator(SourceProductionContext context, INamedTypeSymbol target, TypeHelper typeHelper)
    {
        _typeHelper = typeHelper;
        _compilation = typeHelper.Compilation;
        _context = context;
        _target = target;
        _builder = new(target.ContainingNamespace + ".Rpc");
    }

    public static string? Generate(SourceProductionContext context, INamedTypeSymbol target, TypeHelper typeHelper)
    {
        var generator = new RpcGenerator(context, target, typeHelper);
        return generator.Generate();
    }

    public string? Generate()
    {
        _builder.AppendLine($"internal class {_target.Name}Dispatcher : Ookii.Jumbo.Rpc.IRpcDispatcher");
        _builder.OpenBlock();
        foreach (var member in _target.GetMembers())
        {
            GenerateMemberDispatchMethod(member);
        }

        _builder.AppendLine("void Ookii.Jumbo.Rpc.IRpcDispatcher.Dispatch(string operationName, object target, System.IO.BinaryReader reader, System.IO.BinaryWriter writer)");
        _builder.OpenBlock();
        _builder.AppendLine("switch (operationName)");
        _builder.OpenBlock();
        foreach (var member in _target.GetMembers())
        {
            if (member.Kind == SymbolKind.Method)
            {
                _builder.AppendCaseLabel($"case \"{member.Name}\":");
                _builder.AppendLine($"{member.Name}(({_target.ToQualifiedName()})target, reader, writer);");
                _builder.AppendLine("break;");
                _builder.AppendLine();
            }
        }

        _builder.AppendCaseLabel("default:");
        _builder.AppendLine($"throw new System.MissingMemberException(\"{_target.ToDisplayString()}\", operationName);");
        _builder.CloseBlock(); // switch
        _builder.CloseBlock(); // method
        _builder.CloseBlock(); // class
        return _builder.GetSource();
    }

    public void GenerateMemberDispatchMethod(ISymbol member)
    {
        if (member is not IMethodSymbol method)
        {
            return;
        }

        _builder.AppendLine($"private static void {method.Name}({_target.ToQualifiedName()} __target, System.IO.BinaryReader __reader, System.IO.BinaryWriter __writer)");
        _builder.OpenBlock();
        foreach (var param in method.Parameters)
        {
            var typeName = param.Type.WithNullableAnnotation(NullableAnnotation.NotAnnotated).ToQualifiedName();
            if (param.Type.AllowsNull())
            {
                _builder.AppendLine($"var {param.Name} = __reader.ReadBoolean() ? Ookii.Jumbo.IO.ValueWriter<{typeName}>.ReadValue(__reader) : null;");
            }
            else
            {
                _builder.AppendLine($"var {param.Name} = Ookii.Jumbo.IO.ValueWriter<{typeName}>.ReadValue(__reader);");
            }
        }

        if (!method.ReturnsVoid)
        {
            _builder.Append("var __methodReturnValue = ");
        }

        switch (method.MethodKind)
        {
        case MethodKind.PropertyGet:
            _builder.AppendLine($"__target.{method.AssociatedSymbol!.Name};");
            break;

        case MethodKind.PropertySet:
            _builder.AppendLine($"__target.{method.AssociatedSymbol!.Name} = value;");
            break;

        default:
            _builder.Append($"__target.{method.Name}(");
            foreach (var param in method.Parameters)
            {
                _builder.AppendArgument(param.Name);
            }

            _builder.CloseArgumentList();
            break;
        }

        if (method.ReturnsVoid)
        {
            _builder.AppendLine("__writer.Write((int)Ookii.Jumbo.Rpc.RpcResponseStatus.SuccessNoValue);");
        }
        else
        {
            _builder.AppendLine("__writer.Write((int)Ookii.Jumbo.Rpc.RpcResponseStatus.Success);");
            if (method.ReturnType.AllowsNull())
            {
                _builder.AppendLine($"if (__methodReturnValue == null)");
                _builder.OpenBlock();
                _builder.AppendLine("__writer.Write(false);");
                _builder.CloseBlock();
                _builder.AppendLine("else");
                _builder.OpenBlock();
                _builder.AppendLine("__writer.Write(true);");
            }

            _builder.AppendLine("Ookii.Jumbo.IO.ValueWriter.WriteValue(__methodReturnValue, __writer);");
            if (method.ReturnType.AllowsNull())
            {
                _builder.CloseBlock();
            }
        }

        _builder.CloseBlock(); // method
        _builder.AppendLine();
    }
}
