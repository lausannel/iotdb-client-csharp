/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using System;
using System.Text;
#if NET461
using System.Collections.Concurrent;
#else
using System.Buffers;
#endif

namespace Apache.IoTDB.DataStructure
{
    public sealed class PooledByteBuffer : IDisposable
    {
        private byte[] _buffer;
        private int _writePos;
        private bool _ownsBuffer;
        private bool _disposed;

#if NET461
        private static readonly ConcurrentBag<byte[]> Pool = new ConcurrentBag<byte[]>();

        private static byte[] Rent(int size)
        {
            while (Pool.TryTake(out var buffer))
            {
                if (buffer.Length >= size)
                {
                    return buffer;
                }
            }
            return new byte[size];
        }

        private static void Return(byte[] buffer)
        {
            Pool.Add(buffer);
        }
#else
        private static byte[] Rent(int size)
        {
            return ArrayPool<byte>.Shared.Rent(size);
        }

        private static void Return(byte[] buffer)
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
#endif

        public PooledByteBuffer(int reserve = 1)
        {
            if (reserve < 1)
            {
                reserve = 1;
            }

            _buffer = Rent(reserve);
            _ownsBuffer = true;
        }

        public PooledByteBuffer(byte[] buffer)
        {
            _buffer = buffer ?? throw new ArgumentNullException(nameof(buffer));
            _ownsBuffer = false;
        }

        public int Length => _writePos;

        public void Reset()
        {
            _writePos = 0;
        }

        public void ReplaceBuffer(byte[] buffer, bool ownsBuffer)
        {
            if (buffer == null)
            {
                throw new ArgumentNullException(nameof(buffer));
            }

            ReleaseBuffer();
            _buffer = buffer;
            _ownsBuffer = ownsBuffer;
            _writePos = 0;
        }

        public void EnsureCapacity(int spaceNeed)
        {
            if (spaceNeed < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(spaceNeed));
            }

            if (_writePos + spaceNeed <= _buffer.Length)
            {
                return;
            }

            var newSize = Math.Max(_buffer.Length * 2, _writePos + spaceNeed);
            var newBuffer = Rent(newSize);
            Buffer.BlockCopy(_buffer, 0, newBuffer, 0, _writePos);
            ReleaseBuffer();
            _buffer = newBuffer;
            _ownsBuffer = true;
        }

        public ArraySegment<byte> WrittenSegment => new ArraySegment<byte>(_buffer, 0, _writePos);

        public byte[] ToArray()
        {
            var result = new byte[_writePos];
            Buffer.BlockCopy(_buffer, 0, result, 0, _writePos);
            return result;
        }

        public byte[] GetBufferUnsafe()
        {
            return _buffer;
        }

        private void ReleaseBuffer()
        {
            if (_ownsBuffer && _buffer.Length > 0)
            {
                Return(_buffer);
            }

            _buffer = Array.Empty<byte>();
            _ownsBuffer = false;
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            ReleaseBuffer();
            _disposed = true;
        }

        public void AddBool(bool value)
        {
            AddByte(value ? (byte)1 : (byte)0);
        }

        public void AddInt(int value)
        {
            EnsureCapacity(4);
            _buffer[_writePos] = (byte)(value >> 24);
            _buffer[_writePos + 1] = (byte)(value >> 16);
            _buffer[_writePos + 2] = (byte)(value >> 8);
            _buffer[_writePos + 3] = (byte)value;
            _writePos += 4;
        }

        public void AddLong(long value)
        {
            EnsureCapacity(8);
            _buffer[_writePos] = (byte)(value >> 56);
            _buffer[_writePos + 1] = (byte)(value >> 48);
            _buffer[_writePos + 2] = (byte)(value >> 40);
            _buffer[_writePos + 3] = (byte)(value >> 32);
            _buffer[_writePos + 4] = (byte)(value >> 24);
            _buffer[_writePos + 5] = (byte)(value >> 16);
            _buffer[_writePos + 6] = (byte)(value >> 8);
            _buffer[_writePos + 7] = (byte)value;
            _writePos += 8;
        }

        public void AddFloat(float value)
        {
            var floatBuff = BitConverter.GetBytes(value);
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(floatBuff);
            }

            EnsureCapacity(floatBuff.Length);
            Buffer.BlockCopy(floatBuff, 0, _buffer, _writePos, floatBuff.Length);
            _writePos += floatBuff.Length;
        }

        public void AddDouble(double value)
        {
            var bits = BitConverter.DoubleToInt64Bits(value);
            AddLong(bits);
        }

        public void AddStr(string value)
        {
            if (value == null)
            {
                value = string.Empty;
            }

            var byteCount = Encoding.UTF8.GetByteCount(value);
            AddInt(byteCount);
            EnsureCapacity(byteCount);
            Encoding.UTF8.GetBytes(value, 0, value.Length, _buffer, _writePos);
            _writePos += byteCount;
        }

        public void AddBinary(byte[] value)
        {
            if (value == null)
            {
                value = Array.Empty<byte>();
            }

            AddInt(value.Length);
            EnsureCapacity(value.Length);
            Buffer.BlockCopy(value, 0, _buffer, _writePos, value.Length);
            _writePos += value.Length;
        }

        public void AddChar(char value)
        {
            var charValue = (ushort)value;
            EnsureCapacity(2);
            _buffer[_writePos] = (byte)(charValue >> 8);
            _buffer[_writePos + 1] = (byte)charValue;
            _writePos += 2;
        }

        public void AddByte(byte value)
        {
            EnsureCapacity(1);
            _buffer[_writePos] = value;
            _writePos += 1;
        }
    }
}
