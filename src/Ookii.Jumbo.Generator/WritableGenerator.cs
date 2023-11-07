using Microsoft.CodeAnalysis;

namespace Ookii.Jumbo.Generator;

internal class WritableGenerator
{
    private readonly TypeHelper _typeHelper;
    private readonly Compilation _compilation;
    private readonly SourceProductionContext _context;
    private readonly INamedTypeSymbol _writableClass;
    private readonly SourceBuilder _builder;

    public WritableGenerator(SourceProductionContext context, INamedTypeSymbol writableClass, TypeHelper typeHelper)
    {
        _typeHelper = typeHelper;
        _compilation = typeHelper.Compilation;
        _context = context;
        _writableClass = writableClass;
        _builder = new(writableClass.ContainingNamespace);
    }

    public static string? Generate(SourceProductionContext context, INamedTypeSymbol argumentsClass, TypeHelper typeHelper)
    {
        var generator = new WritableGenerator(context, argumentsClass, typeHelper);
        return generator.Generate();
    }

    public string? Generate()
    {
        string extraKeyword = string.Empty;
        bool overrideMethod = false;
        if (_writableClass.BaseType?.ImplementsInterface(_typeHelper.IWritable) ?? false)
        {
            extraKeyword = "override ";
            overrideMethod = true;
        }
        else
        {
            var attribute = _writableClass.GetAttribute(_typeHelper.GeneratedWritableAttribute!)!;
            if ((bool?)attribute.GetNamedArgument("Virtual")?.Value ?? false)
            {
                extraKeyword = "virtual ";
            }
        }

        _builder.Append($"partial class {_writableClass.Name}");
        if (!overrideMethod)
        {
            _builder.Append(" : Ookii.Jumbo.IO.IWritable");
        }

        _builder.AppendLine();
        _builder.OpenBlock();
        GenerateWriteMethod(extraKeyword, overrideMethod);
        _builder.AppendLine();
        GenerateReadMethod(extraKeyword, overrideMethod);
        _builder.CloseBlock(); // class
        return _builder.GetSource();
    }

    private void GenerateWriteMethod(string extraKeyword, bool overrideMethod)
    {
        _builder.AppendLine("/// <inheritdoc />");
        _builder.AppendLine($"public {extraKeyword}void Write(System.IO.BinaryWriter writer)");
        _builder.OpenBlock();
        if (overrideMethod)
        {
            var baseMethod = _writableClass.BaseType!.GetMember("Write");

            // Attempt to call if null because that means the base class method may have been
            // generated.
            if (baseMethod == null || !baseMethod.IsAbstract)
            {
                _builder.AppendLine("base.Write(writer);");
            }
        }
        else
        {
            _builder.AppendLine("System.ArgumentNullException.ThrowIfNull(writer);");
        }

        for (var current = _writableClass;
             current != null && current.SpecialType == SpecialType.None;
             current = current.BaseType)
        {
            foreach (var member in current.GetMembers())
            {
                GenerateMemberSerialization(member);
            }

            if (overrideMethod)
            {
                break;
            }
        }

        _builder.CloseBlock(); // Write method
    }

    private void GenerateReadMethod(string extraKeyword, bool overrideMethod)
    {
        _builder.AppendLine("/// <inheritdoc />");
        _builder.AppendLine($"public {extraKeyword}void Read(System.IO.BinaryReader reader)");
        _builder.OpenBlock();
        if (overrideMethod)
        {
            var baseMethod = _writableClass.BaseType!.GetMember("Read");
            
            // Attempt to call if null because that means the base class method may have been
            // generated.
            if (baseMethod == null || !baseMethod.IsAbstract)
            {
                _builder.AppendLine("base.Read(reader);");
            }
        }
        else
        {
            _builder.AppendLine("System.ArgumentNullException.ThrowIfNull(reader);");
        }

        for (var current = _writableClass;
             current != null && current.SpecialType == SpecialType.None;
             current = current.BaseType)
        {
            foreach (var member in current.GetMembers())
            {
                GenerateMemberDeserialization(member);
            }

            if (overrideMethod)
            {
                break;
            }
        }

        _builder.CloseBlock(); // Write method
    }

    private void GenerateMemberSerialization(ISymbol member)
    {
        if (!ShouldSerialize(member, out var property))
        {
            return;
        }

        if (property.Type.AllowsNull() && property.GetAttribute(_typeHelper.WritableNotNullAttribute!) == null)
        {
            _builder.AppendLine($"Ookii.Jumbo.IO.ValueWriter.WriteNullable(this.{property.Name}, writer);");
        }
        else if (property.Type.ImplementsInterface(_typeHelper.IWritable))
        {
            _builder.AppendLine($"((Ookii.Jumbo.IO.IWritable)this.{property.Name}).Write(writer);");
        }
        else
        {
            _builder.AppendLine($"Ookii.Jumbo.IO.ValueWriter.WriteValue(this.{property.Name}, writer);");
        }
    }

    private bool ShouldSerialize(ISymbol member, out IPropertySymbol property)
    {
        property = (member as IPropertySymbol)!;
        return property != null && property.GetMethod != null && property.SetMethod != null &&
                    property.GetAttribute(_typeHelper.WritableIgnoreAttribute!) == null;
    }

    private void GenerateMemberDeserialization(ISymbol member)
    {
        if (!ShouldSerialize(member, out var property))
        {
            return;
        }

        var type = property.Type.WithNullableAnnotation(NullableAnnotation.NotAnnotated).GetUnderlyingType();
        if (property.Type.AllowsNull() && property.GetAttribute(_typeHelper.WritableNotNullAttribute!) == null)
        {
            if (property.Type.IsNullableValueType())
            {
                _builder.AppendLine($"this.{property.Name} = Ookii.Jumbo.IO.ValueWriter.ReadNullableStruct<{type.ToQualifiedName()}>(reader);");
            }
            else
            {
                _builder.AppendLine($"this.{property.Name} = Ookii.Jumbo.IO.ValueWriter.ReadNullable<{type.ToQualifiedName()}>(reader);");
            }
        }
        else if (property.Type.ImplementsInterface(_typeHelper.IWritable))
        {
            _builder.AppendLine($"if (this.{property.Name} == null)"); 
            _builder.OpenBlock();
            _builder.AppendLine($"this.{property.Name} = Ookii.Jumbo.IO.WritableUtility.GetUninitializedWritable<{type.ToQualifiedName()}>();");
            _builder.CloseBlock();
            _builder.AppendLine();
            _builder.AppendLine($"((Ookii.Jumbo.IO.IWritable)this.{property.Name}).Read(reader);");
        }
        else
        {
            _builder.AppendLine($"this.{property.Name} = Ookii.Jumbo.IO.ValueWriter<{type.ToQualifiedName()}>.ReadValue(reader);");
        }
    }

}
