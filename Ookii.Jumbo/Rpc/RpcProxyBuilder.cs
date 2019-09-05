// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection.Emit;
using System.Reflection;

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
            lock( _proxies )
            {
                if( !_proxies.TryGetValue(interfaceType, out proxyType) )
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
            if( interfaceType == null )
                throw new ArgumentNullException("interfaceType");
            if( !interfaceType.IsInterface )
                throw new ArgumentException("Type is not an interface.", "interfaceType");
            if( interfaceType.IsGenericType || interfaceType.IsGenericTypeDefinition )
                throw new ArgumentException("Generic types are not supported.");

            TypeBuilder proxyType = _proxyModule.DefineType("Ookii.Jumbo.Rpc.DynamicProxy." + interfaceType.FullName.Replace('.', '_').Replace('+', '_'), TypeAttributes.Class | TypeAttributes.Sealed | TypeAttributes.Public | TypeAttributes.BeforeFieldInit, typeof(RpcProxyBase), new[] { interfaceType });

            CreateConstructor(proxyType, interfaceType);

            foreach( MemberInfo member in interfaceType.GetMembers() )
            {
                switch( member.MemberType )
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
            if( interfaceMethod.IsGenericMethod || interfaceMethod.IsGenericMethodDefinition )
                throw new NotSupportedException("Generic methods are not supported.");

            ParameterInfo[] parameters = interfaceMethod.GetParameters();
            Type[] parameterTypes = new Type[parameters.Length];
            for( int x = 0; x < parameters.Length; ++x )
                parameterTypes[x] = parameters[x].ParameterType;
            MethodAttributes attributes = interfaceMethod.Attributes & ~(MethodAttributes.Abstract) | MethodAttributes.Virtual | MethodAttributes.Final;
            MethodBuilder proxyMethod = proxyType.DefineMethod(interfaceMethod.Name, attributes, interfaceMethod.CallingConvention, interfaceMethod.ReturnType, parameterTypes);
            foreach( ParameterInfo param in parameters )
            {
                if( param.ParameterType.IsByRef )
                    throw new NotSupportedException("Interface methods with reference parameters are not supported.");
                proxyMethod.DefineParameter(param.Position + 1, param.Attributes, param.Name);
            }

            ILGenerator generator = proxyMethod.GetILGenerator();
            generator.DeclareLocal(typeof(object[]));
            generator.Emit(OpCodes.Ldarg_0); // Load "this" (for the SendRequest call later)
            generator.Emit(OpCodes.Ldstr, interfaceMethod.Name); // Load the method name
            if( parameters.Length == 0 )
                generator.Emit(OpCodes.Ldnull);
            else
            {
                generator.Emit(OpCodes.Ldc_I4, parameters.Length); // Load the number of parameters
                generator.Emit(OpCodes.Newarr, typeof(object)); // Create a new array
                generator.Emit(OpCodes.Stloc_0); // Store the array
                for( int x = 0; x < parameters.Length; ++x )
                {
                    generator.Emit(OpCodes.Ldloc_0); // Load the array
                    generator.Emit(OpCodes.Ldc_I4, x); // Load the index
                    generator.Emit(OpCodes.Ldarg_S, x + 1); // Load the argument
                    if( parameters[x].ParameterType.IsValueType )
                        generator.Emit(OpCodes.Box, parameters[x].ParameterType); // Box the argument if it's a value type
                    generator.Emit(OpCodes.Stelem_Ref); // Store the argument in the array at the specified index.
                }

                generator.Emit(OpCodes.Ldloc_0); // Load the array
            }
            MethodInfo sendRequestMethod = typeof(RpcProxyBase).GetMethod("SendRequest", BindingFlags.NonPublic | BindingFlags.Instance);
            generator.Emit(OpCodes.Call, sendRequestMethod); // Call the SendRequest method

            if( interfaceMethod.ReturnType == typeof(void) )
                generator.Emit(OpCodes.Pop); // Pop the return value off the stack
            else if( interfaceMethod.ReturnType.IsValueType )
                generator.Emit(OpCodes.Unbox_Any, interfaceMethod.ReturnType); // Unbox the return type
            else if( interfaceMethod.ReturnType != typeof(object) )
                generator.Emit(OpCodes.Castclass, interfaceMethod.ReturnType); // Cast the return type

            generator.Emit(OpCodes.Ret);

            return proxyMethod;
        }

        private static void CreateProperty(TypeBuilder proxyType, PropertyInfo interfaceProperty)
        {
            ParameterInfo[] indexParameters = interfaceProperty.GetIndexParameters();
            Type[] parameterTypes = new Type[indexParameters.Length];
            for( int x = 0; x < indexParameters.Length; ++x )
                parameterTypes[x] = indexParameters[x].ParameterType;
            PropertyBuilder proxyProperty = proxyType.DefineProperty(interfaceProperty.Name, interfaceProperty.Attributes, interfaceProperty.PropertyType, parameterTypes);

            if( interfaceProperty.CanRead )
            {
                MethodBuilder getMethod = CreateMethod(proxyType, interfaceProperty.GetGetMethod());
                proxyProperty.SetGetMethod(getMethod);
            }

            if( interfaceProperty.CanWrite )
            {
                MethodBuilder setMethod = CreateMethod(proxyType, interfaceProperty.GetSetMethod());
                proxyProperty.SetSetMethod(setMethod);
            }
        }

        private static void CreateConstructor(TypeBuilder proxyType, Type interfaceType)
        {
            ConstructorBuilder ctor = proxyType.DefineConstructor(MethodAttributes.Public, CallingConventions.HasThis, new[] { typeof(string), typeof(int), typeof(string) });
            ctor.DefineParameter(1, ParameterAttributes.In, "hostName");
            ctor.DefineParameter(2, ParameterAttributes.In, "port");
            ctor.DefineParameter(3, ParameterAttributes.In, "objectName");

            ILGenerator generator = ctor.GetILGenerator();
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
