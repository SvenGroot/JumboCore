using System;
using System.IO;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Rpc;

partial class RpcRemoteException
{
    private class MessageOnlyExceptionWriter<T> : IValueWriter<Exception>
        where T : Exception
    {
        public Exception Read(BinaryReader reader)
        {
            var message = reader.ReadString();
            return (T)Activator.CreateInstance(typeof(T), message)!;
        }

        public void Write(Exception value, BinaryWriter writer)
        {
            var ex = (T)value;
            writer.Write(ex.Message);
        }
    }

    private class ArgumentNullExceptionWriter : IValueWriter<Exception>
    {
        public Exception Read(BinaryReader reader)
        {
            var message = reader.ReadString();
            string? paramName = null;
            if (reader.ReadBoolean())
            {
                paramName = reader.ReadString();
            }

            return new ArgumentNullException(paramName, message);
        }

        public void Write(Exception value, BinaryWriter writer)
        {
            var ex = (ArgumentNullException)value;
            writer.Write(ex.Message);
            writer.Write(ex.ParamName != null);
            if (ex.ParamName != null)
            {
                writer.Write(ex.ParamName);
            }
        }
    }
}
