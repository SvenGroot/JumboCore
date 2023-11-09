// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.Serialization.Formatters.Binary;
using Ookii.Jumbo.Jet.Tasks;

namespace Ookii.Jumbo.Jet.Jobs.Builder;

/// <summary>
/// Allows you to create task classes from static methods.
/// </summary>
public sealed class DynamicTaskBuilder
{
    private AssemblyBuilder? _assembly;
    private ModuleBuilder? _module;
    private string? _dynamicAssemblyDirectory;
    private readonly Dictionary<Tuple<MethodInfo, Delegate, int, RecordReuseMode>, Type> _taskTypeCache = new Dictionary<Tuple<MethodInfo, Delegate, int, RecordReuseMode>, Type>();
    private readonly HashSet<string> _usedTypeNames = new HashSet<string>();

    /// <summary>
    /// Gets a value indicating whether a dynamic assembly has been created.
    /// </summary>
    /// <value>
    /// 	<see langword="true"/> if a dynamic assembly has been created.; otherwise, <see langword="false"/>.
    /// </value>
    [MemberNotNullWhen(true, nameof(_assembly))]
    [MemberNotNullWhen(true, nameof(_module))]
    [MemberNotNullWhen(true, nameof(_dynamicAssemblyDirectory))]
    [MemberNotNullWhen(true, nameof(DynamicAssemblyFileName))]
    [MemberNotNullWhen(true, nameof(DynamicAssemblyPath))]
    public bool IsDynamicAssemblyCreated
    {
        get { return _assembly != null; }
    }

    /// <summary>
    /// Gets the name of the dynamic assembly file.
    /// </summary>
    /// <value>
    /// The name of the dynamic assembly file, or <see langword="null"/> if no assembly has been generated.
    /// </value>
    public string? DynamicAssemblyFileName
    {
        get { return IsDynamicAssemblyCreated ? _assembly.GetName().Name + ".dll" : null; }
    }

    /// <summary>
    /// Gets the full path of the dynamic assembly file.
    /// </summary>
    /// <value>
    /// The full path of the dynamic assembly file, or <see langword="null"/> if no assembly has been generated.
    /// </value>
    public string? DynamicAssemblyPath
    {
        get { return IsDynamicAssemblyCreated ? Path.Combine(_dynamicAssemblyDirectory, DynamicAssemblyFileName) : null; }
    }

    /// <summary>
    /// Creates a dynamically generated task class by overriding the specified method.
    /// </summary>
    /// <param name="methodToOverride">The method to override.</param>
    /// <param name="taskMethodDelegate">Delegate for the method that the implementation of <paramref name="methodToOverride"/> will call.</param>
    /// <param name="skipParameters">The number of parameters of <paramref name="methodToOverride"/> to skip before passing parameters on to the delegate method.</param>
    /// <param name="recordReuseMode">The record reuse mode.</param>
    /// <returns>The <see cref="Type"/> instance for the dynamically generated type.</returns>
    /// <remarks>
    /// <para>
    ///   The <see cref="MemberInfo.DeclaringType"/> of <paramref name="methodToOverride"/> will become the base type of the dynamic task type. If the <see cref="MemberInfo.DeclaringType"/> is
    ///   an interface, the base type will be <see cref="Configurable"/> and the type will implement the specified interface. The interface or base type
    ///   may not have any other methods that need to be overridden.
    /// </para>
    /// <para>
    ///   The target method for <paramref name="taskMethodDelegate"/> must match the signature of the <paramref name="methodToOverride"/>, minus
    ///   <paramref name="skipParameters"/> parameters at the start. It may optionally take an additional parameter of type <see cref="TaskContext"/>.
    /// </para>
    /// <para>
    ///   The target method for <paramref name="taskMethodDelegate"/> must be a public static
    ///   method. Note that lambda expressions never create a public static method, even if
    ///   they capture no state or the <c>static</c> keyword is used.
    /// </para>
    /// <para>
    ///   If <paramref name="recordReuseMode"/> is <see cref="RecordReuseMode.Default"/> and the
    ///   target method has the <see cref="AllowRecordReuseAttribute"/> attribute applied to it,
    ///   that attribute will be copied to the task class. If the target method has the
    ///   <see cref="ProcessAllInputPartitionsAttribute"/> attribute applied to it, that
    ///   attribute will be copied to the task class.
    /// </para>
    /// </remarks>
    public Type CreateDynamicTask(MethodInfo methodToOverride, Delegate taskMethodDelegate, int skipParameters, RecordReuseMode recordReuseMode)
    {
        ArgumentNullException.ThrowIfNull(methodToOverride);
        if (methodToOverride.DeclaringType!.FindGenericInterfaceType(typeof(ITask<,>), false) == null)
            throw new ArgumentException("The method that declares the method to override is not a task.", nameof(methodToOverride));
        ArgumentNullException.ThrowIfNull(taskMethodDelegate);

        var cacheKey = Tuple.Create(methodToOverride, taskMethodDelegate, skipParameters, recordReuseMode);
        if (_taskTypeCache.TryGetValue(cacheKey, out var cachedTask))
            return cachedTask;

        var parameters = methodToOverride.GetParameters();
        var delegateParameters = taskMethodDelegate.Method.GetParameters();
        if (methodToOverride.ReturnType != taskMethodDelegate.Method.ReturnType)
            throw new ArgumentException("The delegate method doesn't have the correct return type.");
        ValidateParameters(skipParameters, parameters, delegateParameters);

        var taskType = CreateTaskType(taskMethodDelegate, recordReuseMode, methodToOverride.DeclaringType!);
        var overriddenMethod = OverrideMethod(taskType, methodToOverride);

        var generator = overriddenMethod.GetILGenerator();
        for (var x = skipParameters; x < parameters.Length; ++x)
            generator.Emit(OpCodes.Ldarg, x + 1); // Zero is "this", hence +1
        if (delegateParameters.Length > parameters.Length - skipParameters)
        {
            // Put the TaskContext on the stack.
            generator.Emit(OpCodes.Ldarg_0);
            generator.Emit(OpCodes.Call, typeof(Configurable).GetProperty("TaskContext")!.GetGetMethod()!);
        }
        generator.Emit(OpCodes.Call, taskMethodDelegate.Method);
        generator.Emit(OpCodes.Ret);

        var result = taskType.CreateType()!;

        _taskTypeCache.Add(cacheKey, result);

        return result;
    }

    /// <summary>
    /// Saves the dynamic assembly, if one was created.
    /// </summary>
    public void SaveAssembly()
    {
        if (IsDynamicAssemblyCreated)
        {
            // TODO: Switch back to _assembly.Save once supported by .Net Core.
            var generator = new Lokad.ILPack.AssemblyGenerator();
            generator.GenerateAssembly(_assembly, Path.Combine(_dynamicAssemblyDirectory, _assembly.GetName().Name + ".dll"));
        }
    }

    /// <summary>
    /// Deletes the dynamic assembly, if it was saved.
    /// </summary>
    public void DeleteAssembly()
    {
        if (IsDynamicAssemblyCreated && File.Exists(DynamicAssemblyPath))
            File.Delete(DynamicAssemblyPath);
    }

    internal bool IsDynamicAssembly(Assembly assembly)
    {
        return object.Equals(assembly, _assembly);
    }

    [MemberNotNull(nameof(_assembly))]
    [MemberNotNull(nameof(_module))]
    [MemberNotNull(nameof(_dynamicAssemblyDirectory))]
    private void CreateDynamicAssembly()
    {
        if (_assembly == null)
        {
            // Use a Guid to ensure a unique name.
            var name = new AssemblyName("Ookii.Jumbo.Jet.Generated." + Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture));
            _dynamicAssemblyDirectory = Path.GetTempPath();
            _assembly = AssemblyBuilder.DefineDynamicAssembly(name, AssemblyBuilderAccess.Run);
            _module = _assembly.DefineDynamicModule(name.Name!);
        }
        else
        {
            Debug.Assert(_module != null);
            Debug.Assert(_dynamicAssemblyDirectory != null);
        }
    }

    private static void ValidateParameters(int skipParameters, ParameterInfo[] parameters, ParameterInfo[] delegateParameters)
    {
        if (skipParameters < 0 || skipParameters > parameters.Length)
            throw new ArgumentOutOfRangeException(nameof(skipParameters));
        if (delegateParameters.Length < parameters.Length - skipParameters || delegateParameters.Length > parameters.Length - skipParameters + 1)
            throw new ArgumentException("The delegate method doesn't have the correct number of parameters.");
        for (var x = 0; x < delegateParameters.Length; ++x)
        {
            var requiredType = (x + skipParameters == parameters.Length) ? typeof(TaskContext) : parameters[x + skipParameters].ParameterType;
            if (delegateParameters[x].ParameterType != requiredType)
                throw new ArgumentException("The delegate method doesn't have the correct method signature.");
        }
    }

    private static void SetTaskAttributes(MethodInfo taskMethod, RecordReuseMode mode, TypeBuilder taskTypeBuilder)
    {
        if (mode != RecordReuseMode.DoNotAllow)
        {
            var allowRecordReuseAttributeType = typeof(AllowRecordReuseAttribute);
            var allowRecordReuse = (AllowRecordReuseAttribute?)Attribute.GetCustomAttribute(taskMethod, allowRecordReuseAttributeType);
            if (mode == RecordReuseMode.Allow || mode == RecordReuseMode.PassThrough || allowRecordReuse != null)
            {
                var ctor = allowRecordReuseAttributeType.GetConstructor(Type.EmptyTypes)!;
                var passThrough = allowRecordReuseAttributeType.GetProperty("PassThrough")!;

                var allowRecordReuseBuilder = new CustomAttributeBuilder(ctor, Array.Empty<object>(), new[] { passThrough }, new object[] { mode == RecordReuseMode.PassThrough || (allowRecordReuse != null && allowRecordReuse.PassThrough) });
                taskTypeBuilder.SetCustomAttribute(allowRecordReuseBuilder);
            }
        }

        if (Attribute.IsDefined(taskMethod, typeof(ProcessAllInputPartitionsAttribute)))
        {
            var ctor = typeof(ProcessAllInputPartitionsAttribute).GetConstructor(Type.EmptyTypes)!;
            var partitionAttribute = new CustomAttributeBuilder(ctor, Array.Empty<object>());

            taskTypeBuilder.SetCustomAttribute(partitionAttribute);
        }
    }

    private TypeBuilder CreateTaskType(Delegate taskDelegate, RecordReuseMode recordReuseMode, Type baseOrInterfaceType)
    {
        CreateDynamicAssembly();

        Type[]? interfaces = null;
        if (baseOrInterfaceType.IsInterface)
        {
            interfaces = new[] { baseOrInterfaceType };
            baseOrInterfaceType = typeof(Configurable);
        }

        var typeName = taskDelegate.Method.Name + "Task";
        var suffix = 2;
        while (_usedTypeNames.Contains(typeName))
        {
            typeName = taskDelegate.Method.Name + "Task" + suffix;
            suffix++;
        }
        _usedTypeNames.Add(typeName);

        var taskTypeBuilder = _module.DefineType(_assembly.GetName().Name + "." + typeName, TypeAttributes.Class | TypeAttributes.Public | TypeAttributes.Sealed, baseOrInterfaceType, interfaces);

        SetTaskAttributes(taskDelegate.Method, recordReuseMode, taskTypeBuilder);

        return taskTypeBuilder;
    }

    private static MethodBuilder OverrideMethod(TypeBuilder taskTypeBuilder, MethodInfo interfaceMethod)
    {
        var parameters = interfaceMethod.GetParameters();
        var method = taskTypeBuilder.DefineMethod(interfaceMethod.Name, MethodAttributes.Public | MethodAttributes.Virtual, interfaceMethod.ReturnType, parameters.Select(p => p.ParameterType).ToArray());
        foreach (var parameter in parameters)
        {
            method.DefineParameter(parameter.Position + 1, parameter.Attributes, parameter.Name);
        }

        return method;
    }
}
