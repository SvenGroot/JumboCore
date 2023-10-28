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
        _builder.AppendLine($"partial class {_writableClass.Name} : Ookii.Jumbo.IO.IWritable");
        _builder.OpenBlock();
        GenerateWriteMethod();
        _builder.AppendLine();
        GenerateReadMethod();
        _builder.CloseBlock(); // class
        return _builder.GetSource();
    }

    private void GenerateWriteMethod()
    {
        _builder.AppendLine("/// <inheritdoc />");
        _builder.AppendLine($"void Ookii.Jumbo.IO.IWritable.Write(System.IO.BinaryWriter writer)");
        _builder.OpenBlock();
        _builder.AppendLine("System.ArgumentNullException.ThrowIfNull(writer);");

        for (var current = _writableClass;
             current != null && current.SpecialType == SpecialType.None;
             current = current.BaseType)
        {
            foreach (var member in current.GetMembers())
            {
                GenerateMemberSerialization(member);
            }
        }

        _builder.CloseBlock(); // Write method
    }

    private void GenerateReadMethod()
    {
        _builder.AppendLine("/// <inheritdoc />");
        _builder.AppendLine($"void Ookii.Jumbo.IO.IWritable.Read(System.IO.BinaryReader reader)");
        _builder.OpenBlock();
        _builder.AppendLine("System.ArgumentNullException.ThrowIfNull(reader);");

        for (var current = _writableClass;
             current != null && current.SpecialType == SpecialType.None;
             current = current.BaseType)
        {
            foreach (var member in current.GetMembers())
            {
                GenerateMemberDeserialization(member);
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

        bool closeIf = false;
        bool nullableValueType = property.Type.IsNullableValueType();
        if ((nullableValueType || property.Type.IsReferenceType)
            && property.GetAttribute(_typeHelper.WritableNotNullAttribute!) == null)
        {
            _builder.AppendLine($"if (this.{property.Name} == null)");
            _builder.OpenBlock();
            _builder.AppendLine("writer.Write(false);");
            _builder.CloseBlock();
            _builder.AppendLine("else");
            _builder.OpenBlock();
            _builder.AppendLine("writer.Write(true);");
            closeIf = true;
        }

        if (property.Type.ImplementsInterface(_typeHelper.IWritable))
        {
            _builder.AppendLine($"((Ookii.Jumbo.IO.IWritable)this.{property.Name}).Write(writer);");
        }
        else if (nullableValueType)
        {
            _builder.AppendLine($"Ookii.Jumbo.IO.ValueWriter.WriteValue(this.{property.Name}!.Value, writer);");
        }
        else
        {
            _builder.AppendLine($"Ookii.Jumbo.IO.ValueWriter.WriteValue(this.{property.Name}, writer);");
        }

        if (closeIf)
        {
            _builder.CloseBlock();
            _builder.AppendLine();
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
        bool closeIf = false;
        if ((property.Type.IsReferenceType || property.Type.IsNullableValueType())
            && property.GetAttribute(_typeHelper.WritableNotNullAttribute!) == null)
        {
            _builder.AppendLine($"if (!reader.ReadBoolean())");
            _builder.OpenBlock();
            _builder.AppendLine($"this.{property.Name} = null;");
            _builder.CloseBlock();
            _builder.AppendLine("else");
            _builder.OpenBlock();
            closeIf = true;
        }

        if (property.Type.ImplementsInterface(_typeHelper.IWritable))
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

        if (closeIf)
        {
            _builder.CloseBlock();
            _builder.AppendLine();
        }
    }

}
