// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Ookii.Jumbo.Jet;

namespace JobServerApplication
{
    sealed class TaskCompletionBroadcaster : IDisposable
    {
        private readonly Socket _socket;
        private readonly byte[] _buffer = new byte[1024];
        private readonly IPEndPoint _broadcastEndPoint;

        public TaskCompletionBroadcaster(string broadcastAddress, int broadcastPort)
        {
            _broadcastEndPoint = new IPEndPoint(IPAddress.Parse(broadcastAddress), broadcastPort);
            _socket = new Socket(_broadcastEndPoint.AddressFamily, SocketType.Dgram, ProtocolType.Udp);
            _socket.EnableBroadcast = true;
            _socket.Blocking = false;
        }

        public void BroadcastTaskCompletion(Guid jobId, TaskAttemptId taskAttemptId, TaskServerInfo taskServer)
        {
            /* The format for this is as follows
             * Bytes 0-15: job ID
             * Byte 16: task ID length (n)
             * Next n bytes: task ID
             * Byte 17+n: task attempt number
             * Byte 17+n+1: task server name length (m)
             * Next m bytes: task server name
             * Next 2 bytes: task server port number
             * Next 2 bytes: task server file channel port number */

            int length = 16;
            Buffer.BlockCopy(jobId.ToByteArray(), 0, _buffer, 0, 16);
            string taskId = taskAttemptId.TaskId.ToString();
            int stringLength = Encoding.UTF8.GetBytes(taskId, 0, taskId.Length, _buffer, length + 1);
            _buffer[length++] = (byte)stringLength;
            length += stringLength;
            _buffer[length++] = (byte)taskAttemptId.Attempt;
            stringLength = Encoding.UTF8.GetBytes(taskServer.Address.HostName, 0, taskServer.Address.HostName.Length, _buffer, length + 1);
            _buffer[length++] = (byte)stringLength;
            length += stringLength;
            _buffer[length++] = (byte)(taskServer.Address.Port & 0xFF);
            _buffer[length++] = (byte)(taskServer.Address.Port >> 8 & 0xFF);
            _buffer[length++] = (byte)(taskServer.FileServerPort & 0xFF);
            _buffer[length++] = (byte)(taskServer.FileServerPort >> 8 & 0xFF);

            _socket.SendTo(_buffer, length, SocketFlags.None, _broadcastEndPoint);
        }

        public void Dispose()
        {
            ((IDisposable)_socket).Dispose();
            GC.SuppressFinalize(this);
        }
    }
}
