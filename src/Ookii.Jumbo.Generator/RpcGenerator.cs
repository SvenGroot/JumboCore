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
        GenerateDispatcher();
        _builder.AppendLine();
        GenerateClient();
        return _builder.GetSource();
    }

    private void GenerateDispatcher()
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
                if (param.Type.IsNullableValueType())
                {
                    _builder.AppendLine($"var {param.Name} = Ookii.Jumbo.IO.ValueWriter.ReadNullableStruct<{typeName}>(__reader);");
                }
                else
                {
                    _builder.AppendLine($"var {param.Name} = Ookii.Jumbo.IO.ValueWriter.ReadNullable<{typeName}>(__reader);");
                }
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
            _builder.AppendLine("__writer.Write((byte)Ookii.Jumbo.Rpc.RpcResponseStatus.SuccessNoValue);");
        }
        else
        {
            // This check is added because NameServer and JobServer don't yet have nullable
            // warnings, so this makes it easier to find issues where they returned a null value but
            // the interface was not annotated to allow it.
            if (method.ReturnType.IsReferenceType && !method.ReturnType.AllowsNull())
            {
                _builder.AppendLine("if (__methodReturnValue == null)");
                _builder.OpenBlock();
                _builder.AppendLine("throw new System.InvalidOperationException(\"Null value not allowed according to protocol.\");");
                _builder.CloseBlock(); // if
            }

            _builder.AppendLine("__writer.Write((byte)Ookii.Jumbo.Rpc.RpcResponseStatus.Success);");
            if (method.ReturnType.AllowsNull())
            {
                _builder.AppendLine("Ookii.Jumbo.IO.ValueWriter.WriteNullable(__methodReturnValue, __writer);");
            }
            else
            {
                _builder.AppendLine("Ookii.Jumbo.IO.ValueWriter.WriteValue(__methodReturnValue, __writer);");
            }
        }

        _builder.CloseBlock(); // method
        _builder.AppendLine();
    }

    private void GenerateClient()
    {
        _builder.AppendLine($"internal class {_target.Name}Client : Ookii.Jumbo.Rpc.RpcProxyBase, {_target.ToQualifiedName()}");
        _builder.OpenBlock();
        _builder.AppendLine($"public {_target.Name}Client(string hostName, int port, string objectName) : base(hostName, port, objectName, typeof({_target.ToQualifiedName()}).AssemblyQualifiedName!) {{ }}");
        _builder.AppendLine();
        foreach (var member in _target.GetMembers())
        {
            if (member is IMethodSymbol method)
            {
                GenerateProxyMethod(method);
            }
            else if (member is IPropertySymbol property)
            {
                GenerateProxyProperty(property);
            }
        }

        _builder.CloseBlock(); // class
    }

    private void GenerateProxyMethod(IMethodSymbol method)
    {
        if (method.MethodKind != MethodKind.Ordinary)
        {
            return;
        }

        _builder.Append($"{method.ReturnType.ToQualifiedName()} {_target.ToDisplayString()}.{method.Name}(");
        foreach (var param in method.Parameters)
        {
            _builder.AppendArgument($"{param.Type.ToQualifiedName()} {param.Name}");
        }

        _builder.CloseArgumentList(false);
        _builder.OpenBlock();
        GenerateProxyMethodBody(method);
        _builder.CloseBlock(); // method
        _builder.AppendLine();
    }

    private void GenerateProxyProperty(IPropertySymbol property)
    {
        _builder.Append($"{property.Type.ToQualifiedName()} {_target.ToDisplayString()}.{property.Name}");
        _builder.OpenBlock();
        if (property.GetMethod != null)
        {
            _builder.AppendLine("get");
            _builder.OpenBlock();
            GenerateProxyMethodBody(property.GetMethod);
            _builder.CloseBlock(); // get
        }

        if (property.SetMethod != null)
        {
            _builder.AppendLine("set");
            _builder.OpenBlock();
            GenerateProxyMethodBody(property.SetMethod);
            _builder.CloseBlock(); // get
        }

        _builder.CloseBlock(); // property
        _builder.AppendLine();
    }

    private void GenerateProxyMethodBody(IMethodSymbol method)
    {
        foreach (var param in method.Parameters)
        {
            if (param.Type.IsReferenceType && param.Type.NullableAnnotation == NullableAnnotation.NotAnnotated)
            {
                _builder.AppendLine($"System.ArgumentNullException.ThrowIfNull({param.Name});");
            }
        }

        if (!method.ReturnsVoid)
        {
            _builder.Append("var __reader = ");
        }

        _builder.Append($"SendRequest(\"{method.Name}\", ");
        if (method.Parameters.Length == 0)
        {
            _builder.AppendLine("null);");
        }
        else
        {
            _builder.AppendLine("__writer =>");
            _builder.OpenBlock();
            foreach (var param in method.Parameters)
            {
                if (param.Type.AllowsNull())
                {
                    _builder.AppendLine($"Ookii.Jumbo.IO.ValueWriter.WriteNullable({param.Name}, __writer);");
                }
                else
                {
                    _builder.AppendLine($"Ookii.Jumbo.IO.ValueWriter.WriteValue({param.Name}, __writer);");
                }
            }

            _builder.DecreaseIndent();
            _builder.AppendLine("});");
        }

        if (!method.ReturnsVoid)
        {
            _builder.AppendLine();
            var typeName = method.ReturnType.WithNullableAnnotation(NullableAnnotation.NotAnnotated).ToQualifiedName();
            if (method.ReturnType.AllowsNull())
            {
                if (method.ReturnType.IsNullableValueType())
                {
                    _builder.AppendLine($"return Ookii.Jumbo.IO.ValueWriter.ReadNullableStruct<{typeName}>(__reader!);");
                }
                else
                {
                    _builder.AppendLine($"return Ookii.Jumbo.IO.ValueWriter.ReadNullable<{typeName}>(__reader!);");
                }
            }
            else
            {
                _builder.AppendLine($"return Ookii.Jumbo.IO.ValueWriter<{typeName}>.ReadValue(__reader!);");
            }
        }
    }
}
