// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Reflection;
using System.Reflection.Emit;
using System.Globalization;

namespace Ookii.Jumbo.IO
{
    /// <summary>
    /// Provides utility functions for creating reflection-based <see cref="IWritable"/> implementations for classes.
    /// </summary>
    public static class WritableUtility
    {
        private static readonly Dictionary<Type, MethodInfo> _readMethods = CreateBinaryReaderMethodTable();

        /// <summary>
        /// Uses reflection to creates a function that serializes an object to a <see cref="BinaryWriter"/>; this function
        /// can be used in a <see cref="IWritable.Write"/> method.
        /// </summary>
        /// <typeparam name="T">The type of the object to serialize.</typeparam>
        /// <returns>A <see cref="Action{T, BinaryWriter}"/> delegate to a method that serializes the object.</returns>
        /// <remarks>
        /// <para>
        ///   The serializer created by this method will serialize only the public properties of the type which have
        ///   a public get and set method. If you need to serialize additional state, you should do that manually.
        /// </para>
        /// <para>
        ///   The serializer supports properties that have a type supported by one of the overloads of the <see cref="BinaryWriter.Write(string)"/>
        ///   method, as well those who implement <see cref="IWritable"/> themselves. The serializer supports
        ///   <see langword="null"/> values by writing a <see cref="Boolean"/> before each property that has a reference type
        ///   that indicates whether it's <see langword="null"/> or not.
        /// </para>
        /// </remarks>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA2204:Literals should be spelled correctly", MessageId = "IWritable"), System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1004:GenericMethodsShouldProvideTypeParameter")]
        public static Action<T, BinaryWriter> CreateSerializer<T>()
        {
            Type type = typeof(T);
            PropertyInfo[] properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);

            DynamicMethod serializer = new DynamicMethod("Write", null, new[] { type, typeof(BinaryWriter) }, type);
            ILGenerator generator = serializer.GetILGenerator();

            WriteArgNullCheck(generator, OpCodes.Ldarg_0, "obj");
            WriteArgNullCheck(generator, OpCodes.Ldarg_1, "writer");

            MethodInfo writeBooleanMethod = typeof(BinaryWriter).GetMethod("Write", new[] { typeof(bool) });

            Dictionary<Type, LocalBuilder> valueWriterLocals = null;

            foreach( PropertyInfo property in properties )
            {
                if( property.CanRead && property.CanWrite && !property.IsSpecialName && !Attribute.IsDefined(property, typeof(WritableIgnoreAttribute)) )
                {
                    MethodInfo getMethod = property.GetGetMethod();
                    if( property.PropertyType.GetInterface(typeof(IWritable).FullName) != null )
                    {
                        generator.Emit(OpCodes.Ldarg_0); // Load the object.
                        generator.Emit(OpCodes.Callvirt, getMethod); // Get the property value.
                        Label? endLabel = WriteCheckForNullIfReferenceType(generator, writeBooleanMethod, property, false, null);
                        generator.Emit(OpCodes.Ldarg_1); // load the writer.
                        generator.Emit(OpCodes.Callvirt, typeof(IWritable).GetMethod("Write", new[] { typeof(BinaryWriter) }));
                        if( endLabel != null )
                        {
                            generator.MarkLabel(endLabel.Value);
                        }
                    }
                    else if( Attribute.IsDefined(property.PropertyType, typeof(ValueWriterAttribute)) )
                    {
                        Type valueWriterType = typeof(IValueWriter<>).MakeGenericType(property.PropertyType);
                        LocalBuilder valueWriterLocal = null;
                        if( valueWriterLocals == null )
                            valueWriterLocals = new Dictionary<Type, LocalBuilder>();

                        if( !valueWriterLocals.TryGetValue(property.PropertyType, out valueWriterLocal) )
                        {
                            // First time using this value writer
                            valueWriterLocal = generator.DeclareLocal(valueWriterType);
                            generator.Emit(OpCodes.Call, typeof(ValueWriter<>).MakeGenericType(property.PropertyType).GetProperty("Writer").GetGetMethod()); // Get the ValueWriter<T>.Writer property value
                            generator.Emit(OpCodes.Stloc_S, valueWriterLocal); // Store the writer in the local.
                            valueWriterLocals.Add(property.PropertyType, valueWriterLocal);
                        }

                        generator.Emit(OpCodes.Ldloc_S, valueWriterLocal); // Load the value writer from the local
                        generator.Emit(OpCodes.Ldarg_0); // put the object on the stack
                        generator.Emit(OpCodes.Callvirt, property.GetGetMethod()); // Get the property value
                        generator.Emit(OpCodes.Ldarg_1); // Put the writer on the stack
                        generator.Emit(OpCodes.Callvirt, valueWriterType.GetMethod("Write")); // Call the IValueWriter<T>.Write method.
                    }
                    else if( property.PropertyType.IsEnum )
                    {
                        MethodInfo writeMethod = typeof(BinaryWriter).GetMethod("Write", new[] { Enum.GetUnderlyingType(property.PropertyType) });
                        if( writeMethod == null )
                            throw new NotSupportedException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "Cannot generate an IWritable.Write implementation for type {0} because property {1} has enum type {2} with unsupported underlying type {3}.", typeof(T), property.Name, property.PropertyType, Enum.GetUnderlyingType(property.PropertyType)));

                        generator.Emit(OpCodes.Ldarg_1);
                        generator.Emit(OpCodes.Ldarg_0);
                        generator.Emit(OpCodes.Callvirt, getMethod);
                        generator.Emit(OpCodes.Callvirt, writeMethod);
                    }
                    else if( property.PropertyType == typeof(DateTime) )
                    {
                        // For DateTimes we need to write the DateTimeKind and the ticks.
                        LocalBuilder dateLocal = generator.DeclareLocal(typeof(DateTime));
                        generator.Emit(OpCodes.Ldarg_0); // put the object on the stack.
                        generator.Emit(OpCodes.Callvirt, property.GetGetMethod()); // Get the property value
                        generator.Emit(OpCodes.Stloc_S, dateLocal); // Store the date in a local
                        generator.Emit(OpCodes.Ldarg_1); // put the writer on the stack
                        generator.Emit(OpCodes.Ldloca_S, dateLocal); // put the address of the date on the stack (has to be the address for a property call to work)
                        generator.Emit(OpCodes.Call, typeof(DateTime).GetProperty("Kind").GetGetMethod()); // Get the DateTimeKind.
                        generator.Emit(OpCodes.Conv_U1); // Convert to a byte.
                        generator.Emit(OpCodes.Callvirt, typeof(BinaryWriter).GetMethod("Write", new[] { typeof(byte) })); // Write the DateTimeKind to the stream.
                        generator.Emit(OpCodes.Ldarg_1); // put the writer on the stack
                        generator.Emit(OpCodes.Ldloca_S, dateLocal); // put the address of the date on the stack (has to be the address for a property call to work)
                        generator.Emit(OpCodes.Call, typeof(DateTime).GetProperty("Ticks").GetGetMethod()); // Get the Ticks.
                        generator.Emit(OpCodes.Callvirt, typeof(BinaryWriter).GetMethod("Write", new[] { typeof(long) })); // write the ticks.
                    }
                    else if( property.PropertyType == typeof(byte[]) )
                    {
                        // Special case for byte[] because there's a BinaryWriter.Write(byte[]) method.
                        // We need to store the size and the data of the byte array.
                        LocalBuilder byteArrayLocal = generator.DeclareLocal(typeof(byte[]));
                        generator.Emit(OpCodes.Ldarg_1); // put the writer on the stack.
                        generator.Emit(OpCodes.Ldarg_0); // put the object on the stack
                        generator.Emit(OpCodes.Callvirt, property.GetGetMethod()); // Get the property value.
                        generator.Emit(OpCodes.Stloc_S, byteArrayLocal); // store it in a local.
                        generator.Emit(OpCodes.Ldloc_S, byteArrayLocal); // load the property value
                        Label? endLabel = WriteCheckForNullIfReferenceType(generator, writeBooleanMethod, property, true, byteArrayLocal);
                        generator.Emit(OpCodes.Ldlen);
                        generator.Emit(OpCodes.Conv_I4);
                        generator.Emit(OpCodes.Call, typeof(WritableUtility).GetMethod("Write7BitEncodedInt32", BindingFlags.Static | BindingFlags.Public, null, new[] { typeof(BinaryWriter), typeof(int) }, null)); // Write length as compressed int.
                        generator.Emit(OpCodes.Ldarg_1); // put the writer on the stack.
                        generator.Emit(OpCodes.Ldloc_S, byteArrayLocal); // put the byte array on the stack.
                        generator.Emit(OpCodes.Callvirt, typeof(BinaryWriter).GetMethod("Write", new[] { typeof(byte[]) })); // Write the array data.
                        if( endLabel != null )
                        {
                            generator.MarkLabel(endLabel.Value);
                        }
                    }
                    else if( property.PropertyType.IsArray )
                    {
                        Type elementType = property.PropertyType.GetElementType();
                        MethodInfo writeMethod = typeof(BinaryWriter).GetMethod("Write", new[] { elementType });
                        if( writeMethod != null )
                        {
                            // We need to store the size and the data of the byte array.
                            LocalBuilder byteArrayLocal = generator.DeclareLocal(property.PropertyType);
                            LocalBuilder lengthLocal = generator.DeclareLocal(typeof(int));
                            LocalBuilder indexLocal = generator.DeclareLocal(typeof(int));
                            generator.Emit(OpCodes.Ldarg_1); // put the writer on the stack.
                            generator.Emit(OpCodes.Ldarg_0); // put the object on the stack
                            generator.Emit(OpCodes.Callvirt, property.GetGetMethod()); // Get the property value.
                            generator.Emit(OpCodes.Stloc_S, byteArrayLocal); // store it in a local.
                            generator.Emit(OpCodes.Ldloc_S, byteArrayLocal); // load the property value
                            Label? endLabel = WriteCheckForNullIfReferenceType(generator, writeBooleanMethod, property, true, byteArrayLocal);
                            generator.Emit(OpCodes.Ldlen); // Get the array length
                            generator.Emit(OpCodes.Conv_I4); // Convert it to Int32.
                            generator.Emit(OpCodes.Stloc_S, lengthLocal); // Store in local
                            generator.Emit(OpCodes.Ldloc, lengthLocal); // Load local
                            generator.Emit(OpCodes.Call, typeof(WritableUtility).GetMethod("Write7BitEncodedInt32", BindingFlags.Static | BindingFlags.Public, null, new[] { typeof(BinaryWriter), typeof(int) }, null)); // Write length as compressed int.
                            Label forLoopStartLabel;
                            Label forLoopEndLabel;
                            EmitForLoopStart(generator, indexLocal, out forLoopStartLabel, out forLoopEndLabel);

                            // for loop body
                            generator.Emit(OpCodes.Ldarg_1); // put the writer on the stack.
                            generator.Emit(OpCodes.Ldloc_S, byteArrayLocal); // put the byte array on the stack.
                            generator.Emit(OpCodes.Ldloc_S, indexLocal); // put the index on the stack
                            generator.Emit(OpCodes.Ldelem, elementType); // Get the element at index.
                            generator.Emit(OpCodes.Callvirt, writeMethod); // Write the array element.

                            EmitForLoopEnd(generator, lengthLocal, indexLocal, forLoopStartLabel, forLoopEndLabel);
                            if( endLabel != null )
                            {
                                generator.MarkLabel(endLabel.Value);
                            }
                        }
                        else
                        {
                            throw new NotSupportedException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "Cannot generate an IWritable.Write implementation for type {0} because property {1} has unsupported type {2}.", typeof(T), property.Name, property.PropertyType));
                        }
                    }
                    else
                    {
                        MethodInfo writeMethod = typeof(BinaryWriter).GetMethod("Write", new[] { property.PropertyType });
                        if( writeMethod != null )
                        {
                            generator.Emit(OpCodes.Ldarg_1);
                            generator.Emit(OpCodes.Ldarg_0);
                            generator.Emit(OpCodes.Callvirt, getMethod);
                            Label? endLabel = WriteCheckForNullIfReferenceType(generator, writeBooleanMethod, property, true, null);
                            generator.Emit(OpCodes.Callvirt, writeMethod);
                            if( endLabel != null )
                            {
                                generator.MarkLabel(endLabel.Value);
                            }
                        }
                        else
                        {
                            throw new NotSupportedException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "Cannot generate an IWritable.Write implementation for type {0} because property {1} has unsupported type {2}.", typeof(T), property.Name, property.PropertyType));
                        }
                    }
                }
            }
            generator.Emit(OpCodes.Ret);

            // Disabled because this method is not exposed in .Net Core.
            //serializer.DefineParameter(1, ParameterAttributes.In, "obj");
            //serializer.DefineParameter(2, ParameterAttributes.In, "writer");

            return (Action<T, BinaryWriter>)serializer.CreateDelegate(typeof(Action<T, BinaryWriter>));
        }

        /// <summary>
        /// Uses reflection to create a function that deserializes an object from a <see cref="BinaryReader"/>; this function
        /// can be used in the object's <see cref="IWritable.Read"/> implementation.
        /// </summary>
        /// <typeparam name="T">The type of the object to deserialize.</typeparam>
        /// <returns>A <see cref="Action{T, BinaryReader}"/> delegate to a method that deserializes the object.</returns>
        /// <remarks>
        /// <para>
        ///   The function returned should only be used to deserialize data created by a function returned by <see cref="CreateSerializer{T}"/>.
        /// </para>
        /// </remarks>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1004:GenericMethodsShouldProvideTypeParameter")]
        public static Action<T, BinaryReader> CreateDeserializer<T>()
        {
            Type type = typeof(T);

            DynamicMethod deserializer = new DynamicMethod("Read", null, new[] { type, typeof(BinaryReader) }, type);
            ILGenerator generator = deserializer.GetILGenerator();

            WriteArgNullCheck(generator, OpCodes.Ldarg_0, "obj");
            WriteArgNullCheck(generator, OpCodes.Ldarg_1, "reader");

            MethodInfo readBooleanMethod = typeof(BinaryReader).GetMethod("ReadBoolean");
            PropertyInfo[] properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);

            Dictionary<Type, LocalBuilder> valueWriterLocals = null;

            foreach( PropertyInfo property in properties )
            {
                if( property.CanWrite && property.CanRead && !property.IsSpecialName && !Attribute.IsDefined(property, typeof(WritableIgnoreAttribute)) )
                {
                    Label? endLabel = null;
                    if( !property.PropertyType.IsValueType && !Attribute.IsDefined(property, typeof(WritableNotNullAttribute)) )
                    {
                        // Read a boolean to see if the property is null.
                        generator.Emit(OpCodes.Ldarg_1); // load the reader.
                        generator.Emit(OpCodes.Callvirt, readBooleanMethod);
                        Label nonNullLabel = generator.DefineLabel();
                        endLabel = generator.DefineLabel();
                        generator.Emit(OpCodes.Brtrue_S, nonNullLabel);
                        // False means that the value is true and was not written.
                        generator.Emit(OpCodes.Ldarg_0); // Load the object.
                        generator.Emit(OpCodes.Ldnull);
                        generator.Emit(OpCodes.Callvirt, property.GetSetMethod(true));
                        generator.Emit(OpCodes.Br_S, endLabel.Value);
                        generator.MarkLabel(nonNullLabel);
                    }

                    if( property.PropertyType.GetInterface(typeof(IWritable).FullName) != null )
                    {
                        LocalBuilder local = generator.DeclareLocal(property.PropertyType);
                        generator.Emit(OpCodes.Ldarg_0);// load the object.
                        generator.Emit(OpCodes.Callvirt, property.GetGetMethod()); // get the current property value.
                        generator.Emit(OpCodes.Stloc, local); // store it
                        generator.Emit(OpCodes.Ldloc, local); // load it.
                        Label nonNullLabel = generator.DefineLabel();
                        generator.Emit(OpCodes.Brtrue_S, nonNullLabel);
                        // Create a new instance if the object is null.
                        generator.Emit(OpCodes.Newobj, property.PropertyType.GetConstructor(Type.EmptyTypes));
                        generator.Emit(OpCodes.Stloc, local); // store it.
                        generator.Emit(OpCodes.Ldarg_0); // load the object.
                        generator.Emit(OpCodes.Ldloc, local); // load the new property object.
                        generator.Emit(OpCodes.Callvirt, property.GetSetMethod(true)); // set the property value.
                        generator.MarkLabel(nonNullLabel);
                        generator.Emit(OpCodes.Ldloc, local); // load tjhe property value.
                        generator.Emit(OpCodes.Ldarg_1); // load the reader.
                        generator.Emit(OpCodes.Callvirt, typeof(IWritable).GetMethod("Read", new[] { typeof(BinaryReader) })); // Read the property from the reader.
                    }
                    else if( Attribute.IsDefined(property.PropertyType, typeof(ValueWriterAttribute)) )
                    {
                        Type valueWriterType = typeof(IValueWriter<>).MakeGenericType(property.PropertyType);
                        LocalBuilder valueWriterLocal = null;
                        if( valueWriterLocals == null )
                            valueWriterLocals = new Dictionary<Type, LocalBuilder>();

                        if( !valueWriterLocals.TryGetValue(property.PropertyType, out valueWriterLocal) )
                        {
                            // First time using this value writer
                            valueWriterLocal = generator.DeclareLocal(valueWriterType);
                            generator.Emit(OpCodes.Call, typeof(ValueWriter<>).MakeGenericType(property.PropertyType).GetProperty("Writer").GetGetMethod()); // Get the ValueWriter<T>.Writer property value
                            generator.Emit(OpCodes.Stloc_S, valueWriterLocal); // Store the writer in the local.
                            valueWriterLocals.Add(property.PropertyType, valueWriterLocal);
                        }

                        generator.Emit(OpCodes.Ldarg_0); // put the object on the stack.
                        generator.Emit(OpCodes.Ldloc_S, valueWriterLocal); // put the value writer on the stack.
                        generator.Emit(OpCodes.Ldarg_1); // put the writer on the stack.
                        generator.Emit(OpCodes.Callvirt, valueWriterType.GetMethod("Read")); // call the IValueWriter<T>.Read method
                        generator.Emit(OpCodes.Callvirt, property.GetSetMethod()); // set the property value
                    }
                    else if( property.PropertyType == typeof(DateTime) )
                    {
                        LocalBuilder kindLocal = generator.DeclareLocal(typeof(DateTimeKind));
                        generator.Emit(OpCodes.Ldarg_0); // put the object ont the stack.
                        generator.Emit(OpCodes.Ldarg_1); // put the reader on the stack.
                        generator.Emit(OpCodes.Callvirt, typeof(BinaryReader).GetMethod("ReadByte")); // read the DateTimeKind
                        generator.Emit(OpCodes.Conv_I4); // convert to int.
                        generator.Emit(OpCodes.Stloc_S, kindLocal); // store it.
                        generator.Emit(OpCodes.Ldarg_1); // put the reader on the stack.
                        generator.Emit(OpCodes.Callvirt, typeof(BinaryReader).GetMethod("ReadInt64")); // read the Ticks.
                        generator.Emit(OpCodes.Ldloc_S, kindLocal); // put the DateTimeKind on the stack.
                        generator.Emit(OpCodes.Newobj, typeof(DateTime).GetConstructor(new[] { typeof(long), typeof(DateTimeKind) })); // Create the DateTime instance.
                        generator.Emit(OpCodes.Callvirt, property.GetSetMethod(true)); // Set the DateTime as the property value.
                    }
                    else if( property.PropertyType == typeof(byte[]) )
                    {
                        generator.Emit(OpCodes.Ldarg_0); // put the object on the stack.
                        generator.Emit(OpCodes.Ldarg_1); // put the reader on the stack.
                        generator.Emit(OpCodes.Ldarg_1); // put the reader on the stack (yes, twice).
                        generator.Emit(OpCodes.Call, typeof(WritableUtility).GetMethod("Read7BitEncodedInt32", BindingFlags.Static | BindingFlags.Public, null, new[] { typeof(BinaryReader) }, null)); // read the length
                        generator.Emit(OpCodes.Callvirt, typeof(BinaryReader).GetMethod("ReadBytes", new[] { typeof(int) })); // read the byte array
                        generator.Emit(OpCodes.Callvirt, property.GetSetMethod(true)); // set the byte array as the property value.
                    }
                    else if( property.PropertyType.IsArray )
                    {
                        Type elementType = property.PropertyType.GetElementType();
                        MethodInfo readMethod = _readMethods[elementType];
                        LocalBuilder lengthLocal = generator.DeclareLocal(typeof(int));
                        LocalBuilder arrayLocal = generator.DeclareLocal(property.PropertyType);
                        LocalBuilder indexLocal = generator.DeclareLocal(typeof(int));
                        generator.Emit(OpCodes.Ldarg_1); // put the reader on the stack.
                        generator.Emit(OpCodes.Call, typeof(WritableUtility).GetMethod("Read7BitEncodedInt32", BindingFlags.Static | BindingFlags.Public, null, new[] { typeof(BinaryReader) }, null)); // read the length
                        generator.Emit(OpCodes.Stloc_S, lengthLocal); // Store the length
                        generator.Emit(OpCodes.Ldloc_S, lengthLocal); // Load the length
                        generator.Emit(OpCodes.Newarr, elementType); // Create a new array
                        generator.Emit(OpCodes.Stloc_S, arrayLocal); // Store the array.
                        Label forLoopStartLabel, forLoopEndLabel;
                        EmitForLoopStart(generator, indexLocal, out forLoopStartLabel, out forLoopEndLabel);

                        // for loop body
                        generator.Emit(OpCodes.Ldloc_S, arrayLocal); // Load the array
                        generator.Emit(OpCodes.Ldloc_S, indexLocal); // Load the length
                        generator.Emit(OpCodes.Ldarg_1); // Load the writer
                        generator.Emit(OpCodes.Callvirt, readMethod); // Read the value
                        generator.Emit(OpCodes.Stelem, elementType); // Store the value in the array

                        EmitForLoopEnd(generator, lengthLocal, indexLocal, forLoopStartLabel, forLoopEndLabel);
                        generator.Emit(OpCodes.Ldarg_0); // Load the this object
                        generator.Emit(OpCodes.Ldloc_S, arrayLocal); // Load the array
                        generator.Emit(OpCodes.Callvirt, property.GetSetMethod(true)); // set the array as the property value.
                    }
                    else
                    {
                        MethodInfo readMethod = property.PropertyType.IsEnum ? _readMethods[Enum.GetUnderlyingType(property.PropertyType)] : _readMethods[property.PropertyType];
                        generator.Emit(OpCodes.Ldarg_0); // load the object.
                        generator.Emit(OpCodes.Ldarg_1); // load the reader.
                        generator.Emit(OpCodes.Callvirt, readMethod);
                        generator.Emit(OpCodes.Callvirt, property.GetSetMethod(true));
                    }

                    if( endLabel != null )
                    {
                        generator.MarkLabel(endLabel.Value);
                    }
                }
            }

            generator.Emit(OpCodes.Ret);

            // Disabled because this method is not exposed in .Net Core.
            //deserializer.DefineParameter(1, ParameterAttributes.In | ParameterAttributes.Out, "obj");
            //deserializer.DefineParameter(2, ParameterAttributes.In, "reader");

            return (Action<T, BinaryReader>)deserializer.CreateDelegate(typeof(Action<T, BinaryReader>));
        }

        private static void WriteArgNullCheck(ILGenerator generator, OpCode ldArgOpCode, string argName)
        {
            Label label = generator.DefineLabel();
            generator.Emit(ldArgOpCode);
            generator.Emit(OpCodes.Brtrue_S, label);
            generator.Emit(OpCodes.Ldstr, argName);
            generator.Emit(OpCodes.Newobj, typeof(ArgumentNullException).GetConstructor(new[] { typeof(string) }));
            generator.Emit(OpCodes.Throw);
            generator.MarkLabel(label);
        }

        private static Label? WriteCheckForNullIfReferenceType(ILGenerator generator, MethodInfo writeBooleanMethod, PropertyInfo property, bool writerIsOnStack, LocalBuilder local)
        {
            Label? endLabel = null;
            if( !property.PropertyType.IsValueType )
            {
                if( local == null )
                {
                    local = generator.DeclareLocal(property.PropertyType);
                    generator.Emit(OpCodes.Stloc_S, local);
                    generator.Emit(OpCodes.Ldloc_S, local);
                }
                Label notNullLabel = generator.DefineLabel();
                generator.Emit(OpCodes.Brtrue_S, notNullLabel); // branch if not null

                // Check if null values are allowed.
                if( !Attribute.IsDefined(property, typeof(WritableNotNullAttribute)) )
                {
                    endLabel = generator.DefineLabel();
                    // This is code for if the value is null;
                    if( !writerIsOnStack )
                        generator.Emit(OpCodes.Ldarg_1); // load the writer
                    generator.Emit(OpCodes.Ldc_I4_0);
                    generator.Emit(OpCodes.Callvirt, writeBooleanMethod);
                    generator.Emit(OpCodes.Br_S, endLabel.Value);
                    generator.MarkLabel(notNullLabel);
                    // Code for if the value is not null
                    if( !writerIsOnStack )
                        generator.Emit(OpCodes.Ldarg_1); // load the writer
                    generator.Emit(OpCodes.Ldc_I4_1);
                    generator.Emit(OpCodes.Callvirt, writeBooleanMethod);
                    if( writerIsOnStack )
                        generator.Emit(OpCodes.Ldarg_1); // load the writer back on the stack.
                }
                else
                {
                    // Null values not allowed, write code to throw na exception.
                    generator.Emit(OpCodes.Ldstr, string.Format(CultureInfo.CurrentCulture, "Property {0} may not be null.", property.Name)); // Load exception message
                    generator.Emit(OpCodes.Newobj, typeof(InvalidOperationException).GetConstructor(new[] { typeof(string) })); // Create exception object
                    generator.Emit(OpCodes.Throw); // Throw exception
                    generator.MarkLabel(notNullLabel);
                }
                generator.Emit(OpCodes.Ldloc_S, local); // put the property value back on the stack
            }
            return endLabel;
        }

        private static Dictionary<Type, MethodInfo> CreateBinaryReaderMethodTable()
        {
            Dictionary<Type, MethodInfo> result = new Dictionary<Type, MethodInfo>();
            MethodInfo[] methods = typeof(BinaryReader).GetMethods(BindingFlags.Instance | BindingFlags.Public);
            foreach( MethodInfo method in methods )
            {
                if( method.Name.StartsWith("Read", StringComparison.Ordinal) && method.Name.Length > 4 && method.GetParameters().Length == 0 )
                {
                    result.Add(method.ReturnType, method);
                }
            }
            return result;
        }

        /// <summary>
        /// Writes a 32-bit integer in a compressed format.
        /// </summary>
        /// <param name="writer">The <see cref="BinaryWriter"/> to write the value to.</param>
        /// <param name="value">The 32-bit integer to be written.</param>
        public static void Write7BitEncodedInt32(BinaryWriter writer, int value)
        {
            if( writer == null )
                throw new ArgumentNullException("writer");
            uint uintValue = (uint)value; // this helps support negative numbers, not really needed but anyway.
            while( uintValue >= 0x80 )
            {
                writer.Write((byte)(uintValue | 0x80));
                uintValue = uintValue >> 7;
            }
            writer.Write((byte)uintValue);
        }

        /// <summary>
        /// Reads in a 32-bit integer in compressed format.
        /// </summary>
        /// <param name="reader">The <see cref="BinaryReader"/> to read the value from.</param>
        /// <returns>A 32-bit integer in compressed format. </returns>
        public static int Read7BitEncodedInt32(BinaryReader reader)
        {
            if( reader == null )
                throw new ArgumentNullException("reader");
            byte currentByte;
            int result = 0;
            int bits = 0;
            do
            {
                if( bits == 35 )
                {
                    throw new FormatException("Invalid 7-bit encoded int.");
                }
                currentByte = reader.ReadByte();
                result |= (currentByte & 0x7f) << bits;
                bits += 7;
            }
            while( (currentByte & 0x80) != 0 );
            return result;
        }

        private static void EmitForLoopEnd(ILGenerator generator, LocalBuilder lengthLocal, LocalBuilder indexLocal, Label forLoopStartLabel, Label forLoopEndLabel)
        {
            // Increment index variable.
            generator.Emit(OpCodes.Ldloc_S, indexLocal); // put the index on the stack
            generator.Emit(OpCodes.Ldc_I4_1); // put constant 1 on the stack
            generator.Emit(OpCodes.Add); // Add them.
            generator.Emit(OpCodes.Stloc_S, indexLocal); // store the result
            // for loop condition.
            generator.MarkLabel(forLoopEndLabel); // start of for-loop condition
            generator.Emit(OpCodes.Ldloc_S, indexLocal); // put the index on the stack
            generator.Emit(OpCodes.Ldloc_S, lengthLocal); // put the length on the stack
            generator.Emit(OpCodes.Blt_S, forLoopStartLabel); // branch to for loop body if index less than length.
        }

        private static void EmitForLoopStart(ILGenerator generator, LocalBuilder indexLocal, out Label forLoopStartLabel, out Label forLoopEndLabel)
        {
            generator.Emit(OpCodes.Ldc_I4_0); // Load constant 0
            generator.Emit(OpCodes.Stloc_S, indexLocal); // Store in index local
            forLoopStartLabel = generator.DefineLabel();
            forLoopEndLabel = generator.DefineLabel();
            generator.Emit(OpCodes.Br_S, forLoopEndLabel); // Branch to loop condition
            generator.MarkLabel(forLoopStartLabel); // Start of for loop body.
        }
    }
}
