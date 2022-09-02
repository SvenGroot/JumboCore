// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Reflection.Emit;

namespace Ookii.Jumbo.Rpc
{
    static class RpcProxyBuilder
    {
        private static readonly AssemblyBuilder _proxyAssembly;
        private static readonly ModuleBuilder _proxyModule;
        private static readonly Dictionary<Type, Type> _proxies = new Dictionary<Type, Type>();

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1810:InitializeReferenceTypeStaticFieldsInline")]
        static RpcProxyBuilder()
        {
            _proxyAssembly = AssemblyBuilder.DefineDynamicAssembly(new AssemblyName("Ookii.Jumbo.Rpc.DynamicProxy"), AssemblyBuilderAccess.Run);
            _proxyModule = _proxyAssembly.DefineDynamicModule("DynamicProxyModule");
        }

        public static object GetProxy(Type interfaceType, string hostName, int port, string objectName)
        {
            Type proxyType;
            lock (_proxies)
            {
                if (!_proxies.TryGetValue(interfaceType, out proxyType))
                {
                    proxyType = CreateProxy(interfaceType);
                    _proxies.Add(interfaceType, proxyType);
                }
            }

            return Activator.CreateInstance(proxyType, hostName, port, objectName);
        }

        // Called inside _proxies lock for thread safety.
        private static Type CreateProxy(Type interfaceType)
        {
            ArgumentNullException.ThrowIfNull(interfaceType);
            if (!interfaceType.IsInterface)
                throw new ArgumentException("Type is not an interface.", nameof(interfaceType));
            if (interfaceType.IsGenericType || interfaceType.IsGenericTypeDefinition)
                throw new ArgumentException("Generic types are not supported.");

            var proxyType = _proxyModule.DefineType("Ookii.Jumbo.Rpc.DynamicProxy." + interfaceType.FullName.Replace('.', '_').Replace('+', '_'), TypeAttributes.Class | TypeAttributes.Sealed | TypeAttributes.Public | TypeAttributes.BeforeFieldInit, typeof(RpcProxyBase), new[] { interfaceType });

            CreateConstructor(proxyType, interfaceType);

            foreach (var member in interfaceType.GetMembers())
            {
                switch (member.MemberType)
                {
                case MemberTypes.Method:
                    CreateMethod(proxyType, (MethodInfo)member);
                    break;
                case MemberTypes.Property:
                    CreateProperty(proxyType, (PropertyInfo)member);
                    break;
                default:
                    throw new NotSupportedException("Interface has unsupported member type.");
                }
            }

            return proxyType.CreateType();
        }

        private static MethodBuilder CreateMethod(TypeBuilder proxyType, MethodInfo interfaceMethod)
        {
            if (interfaceMethod.IsGenericMethod || interfaceMethod.IsGenericMethodDefinition)
                throw new NotSupportedException("Generic methods are not supported.");

            var parameters = interfaceMethod.GetParameters();
            var parameterTypes = new Type[parameters.Length];
            for (var x = 0; x < parameters.Length; ++x)
                parameterTypes[x] = parameters[x].ParameterType;
            var attributes = interfaceMethod.Attributes & ~(MethodAttributes.Abstract) | MethodAttributes.Virtual | MethodAttributes.Final;
            var proxyMethod = proxyType.DefineMethod(interfaceMethod.Name, attributes, interfaceMethod.CallingConvention, interfaceMethod.ReturnType, parameterTypes);
            foreach (var param in parameters)
            {
                if (param.ParameterType.IsByRef)
                    throw new NotSupportedException("Interface methods with reference parameters are not supported.");
                proxyMethod.DefineParameter(param.Position + 1, param.Attributes, param.Name);
            }

            var generator = proxyMethod.GetILGenerator();
            generator.DeclareLocal(typeof(object[]));
            generator.Emit(OpCodes.Ldarg_0); // Load "this" (for the SendRequest call later)
            generator.Emit(OpCodes.Ldstr, interfaceMethod.Name); // Load the method name
            if (parameters.Length == 0)
                generator.Emit(OpCodes.Ldnull);
            else
            {
                generator.Emit(OpCodes.Ldc_I4, parameters.Length); // Load the number of parameters
                generator.Emit(OpCodes.Newarr, typeof(object)); // Create a new array
                generator.Emit(OpCodes.Stloc_0); // Store the array
                for (var x = 0; x < parameters.Length; ++x)
                {
                    generator.Emit(OpCodes.Ldloc_0); // Load the array
                    generator.Emit(OpCodes.Ldc_I4, x); // Load the index
                    generator.Emit(OpCodes.Ldarg_S, x + 1); // Load the argument
                    if (parameters[x].ParameterType.IsValueType)
                        generator.Emit(OpCodes.Box, parameters[x].ParameterType); // Box the argument if it's a value type
                    generator.Emit(OpCodes.Stelem_Ref); // Store the argument in the array at the specified index.
                }

                generator.Emit(OpCodes.Ldloc_0); // Load the array
            }
            var sendRequestMethod = typeof(RpcProxyBase).GetMethod("SendRequest", BindingFlags.NonPublic | BindingFlags.Instance);
            generator.Emit(OpCodes.Call, sendRequestMethod); // Call the SendRequest method

            if (interfaceMethod.ReturnType == typeof(void))
                generator.Emit(OpCodes.Pop); // Pop the return value off the stack
            else if (interfaceMethod.ReturnType.IsValueType)
                generator.Emit(OpCodes.Unbox_Any, interfaceMethod.ReturnType); // Unbox the return type
            else if (interfaceMethod.ReturnType != typeof(object))
                generator.Emit(OpCodes.Castclass, interfaceMethod.ReturnType); // Cast the return type

            generator.Emit(OpCodes.Ret);

            return proxyMethod;
        }

        private static void CreateProperty(TypeBuilder proxyType, PropertyInfo interfaceProperty)
        {
            var indexParameters = interfaceProperty.GetIndexParameters();
            var parameterTypes = new Type[indexParameters.Length];
            for (var x = 0; x < indexParameters.Length; ++x)
                parameterTypes[x] = indexParameters[x].ParameterType;
            var proxyProperty = proxyType.DefineProperty(interfaceProperty.Name, interfaceProperty.Attributes, interfaceProperty.PropertyType, parameterTypes);

            if (interfaceProperty.CanRead)
            {
                var getMethod = CreateMethod(proxyType, interfaceProperty.GetGetMethod());
                proxyProperty.SetGetMethod(getMethod);
            }

            if (interfaceProperty.CanWrite)
            {
                var setMethod = CreateMethod(proxyType, interfaceProperty.GetSetMethod());
                proxyProperty.SetSetMethod(setMethod);
            }
        }

        private static void CreateConstructor(TypeBuilder proxyType, Type interfaceType)
        {
            var ctor = proxyType.DefineConstructor(MethodAttributes.Public, CallingConventions.HasThis, new[] { typeof(string), typeof(int), typeof(string) });
            ctor.DefineParameter(1, ParameterAttributes.In, "hostName");
            ctor.DefineParameter(2, ParameterAttributes.In, "port");
            ctor.DefineParameter(3, ParameterAttributes.In, "objectName");

            var generator = ctor.GetILGenerator();
            generator.Emit(OpCodes.Ldarg_0); // Load "this"
            generator.Emit(OpCodes.Ldarg_1); // Load hostName argument
            generator.Emit(OpCodes.Ldarg_2); // Load port argument
            generator.Emit(OpCodes.Ldarg_3); // Load objectName argument
            generator.Emit(OpCodes.Ldstr, interfaceType.AssemblyQualifiedName);
            generator.Emit(OpCodes.Call, proxyType.BaseType.GetConstructor(BindingFlags.NonPublic | BindingFlags.Instance, null, new[] { typeof(string), typeof(int), typeof(string), typeof(string) }, null)); // Call base class constructor
            generator.Emit(OpCodes.Ret);
        }
    }
}
