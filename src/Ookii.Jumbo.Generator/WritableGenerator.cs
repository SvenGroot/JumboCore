using Microsoft.CodeAnalysis;

namespace Ookii.Jumbo.Generator;

internal class WritableGenerator
{
    private readonly TypeHelper _typeHelper;
    private readonly SourceProductionContext _context;
    private readonly INamedTypeSymbol _writableClass;
    private readonly SourceBuilder _builder;

    public WritableGenerator(SourceProductionContext context, INamedTypeSymbol writableClass, TypeHelper typeHelper)
    {
        _typeHelper = typeHelper;
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
        var attribute = _writableClass.GetAttribute(_typeHelper.GeneratedWritableAttribute!);
        if (attribute != null)
        {
            GenerateWritable(attribute);
        }
        else
        {
            GenerateValueWriter();
        }

        return _builder.GetSource();
    }

    private void GenerateWritable(AttributeData attribute)
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
    }

    private void GenerateValueWriter()
    {
        _builder.AppendLine("[Ookii.Jumbo.IO.ValueWriter(typeof(Writer))]");
        var keyword = _writableClass.IsReferenceType ? "class" : "struct";
        _builder.AppendLine($"partial {keyword} {_writableClass.Name}");
        _builder.OpenBlock();
        _builder.AppendLine($"/// <summary>The value writer for <see cref=\"{_writableClass.Name}\" />.</summary>");
        _builder.AppendLine($"public class Writer : Ookii.Jumbo.IO.IValueWriter<{_writableClass.Name}>");
        _builder.OpenBlock();
        GenerateValueWriterWriteMethod();
        _builder.AppendLine();
        _builder.AppendLine("/// <inheritdoc />");
        _builder.AppendLine($"public {_writableClass.Name} Read(System.IO.BinaryReader reader) => new(reader);");
        _builder.CloseBlock(); // writer class
        _builder.AppendLine();
        GenerateConstructor();
        _builder.CloseBlock(); // class
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

        GenerateAllMembersSerialization(overrideMethod, false);
        _builder.CloseBlock(); // Write method
    }

    private void GenerateValueWriterWriteMethod()
    {
        _builder.AppendLine("/// <inheritdoc />");
        _builder.AppendLine($"public void Write({_writableClass.Name} value, System.IO.BinaryWriter writer)");
        _builder.OpenBlock();
        if (_writableClass.IsReferenceType)
        {
            _builder.AppendLine("System.ArgumentNullException.ThrowIfNull(value);");
        }

        _builder.AppendLine("System.ArgumentNullException.ThrowIfNull(writer);");
        GenerateAllMembersSerialization(false, true);
        _builder.CloseBlock(); // Write method
    }

    private void GenerateAllMembersSerialization(bool overrideMethod, bool valueWriter)
    {
        for (var current = _writableClass;
             current != null && current.SpecialType == SpecialType.None;
             current = current.BaseType)
        {
            foreach (var member in current.GetMembers())
            {
                GenerateMemberSerialization(member, valueWriter);
            }

            if (overrideMethod)
            {
                break;
            }
        }
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

        GenerateAllMembersDeserialization(overrideMethod, false);
        _builder.CloseBlock(); // Write method
    }

    private void GenerateConstructor()
    {
        _builder.AppendLine("/// <inheritdoc />");
        _builder.AppendLine($"private {_writableClass.Name}(System.IO.BinaryReader reader)");
        _builder.OpenBlock();
        _builder.AppendLine("System.ArgumentNullException.ThrowIfNull(reader);");
        GenerateAllMembersDeserialization(false, true);
        _builder.CloseBlock(); // Write method
    }

    private void GenerateAllMembersDeserialization(bool overrideMethod, bool ctor)
    {
        for (var current = _writableClass;
             current != null && current.SpecialType == SpecialType.None;
             current = current.BaseType)
        {
            foreach (var member in current.GetMembers())
            {
                GenerateMemberDeserialization(member, ctor);
            }

            if (overrideMethod)
            {
                break;
            }
        }
    }

    private void GenerateMemberSerialization(ISymbol member, bool valueWriter)
    {
        if (!ShouldSerialize(member, valueWriter, out var property))
        {
            return;
        }

        var prefix = valueWriter ? "value" : "this";
        if (property.Type.AllowsNull() && property.GetAttribute(_typeHelper.WritableNotNullAttribute!) == null)
        {
            _builder.AppendLine($"Ookii.Jumbo.IO.ValueWriter.WriteNullable({prefix}.{property.Name}, writer);");
        }
        else if (property.Type.ImplementsInterface(_typeHelper.IWritable))
        {
            _builder.AppendLine($"((Ookii.Jumbo.IO.IWritable){prefix}.{property.Name}).Write(writer);");
        }
        else
        {
            _builder.AppendLine($"Ookii.Jumbo.IO.ValueWriter.WriteValue({prefix}.{property.Name}, writer);");
        }
    }

    private bool ShouldSerialize(ISymbol member, bool valueWriter, out IPropertySymbol property)
    {
        // IValueWriter should serialize automatic properties without a set, because they can be
        // set in the constructor.
        property = (member as IPropertySymbol)!;
        return property != null && property.GetMethod != null && 
            (property.SetMethod != null || valueWriter && property.IsAutomaticProperty()) &&
            property.GetAttribute(_typeHelper.WritableIgnoreAttribute!) == null;
    }

    private void GenerateMemberDeserialization(ISymbol member, bool valueWriter)
    {
        if (!ShouldSerialize(member, valueWriter, out var property))
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
        else if (property.Type.ImplementsInterface(_typeHelper.IWritable) && !valueWriter)
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
