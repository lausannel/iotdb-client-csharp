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
using Apache.IoTDB.DataStructure;
using NUnit.Framework;

namespace Apache.IoTDB.Tests
{
    [TestFixture]
    public class PooledByteBufferTests
    {
        [Test]
        public void PooledByteBuffer_WritesSameBytesAsByteBuffer()
        {
            var pooled = new PooledByteBuffer(1);
            var legacy = new ByteBuffer(1);

            pooled.AddBool(true);
            legacy.AddBool(true);
            pooled.AddInt(0x01020304);
            legacy.AddInt(0x01020304);
            pooled.AddLong(0x0102030405060708L);
            legacy.AddLong(0x0102030405060708L);
            pooled.AddFloat(1.25f);
            legacy.AddFloat(1.25f);
            pooled.AddDouble(2.5d);
            legacy.AddDouble(2.5d);
            pooled.AddChar('Z');
            legacy.AddChar('Z');
            pooled.AddStr("hi");
            legacy.AddStr("hi");
            pooled.AddBinary(new byte[] { 9, 8, 7 });
            legacy.AddBinary(new byte[] { 9, 8, 7 });

            Assert.That(pooled.ToArray(), Is.EqualTo(legacy.GetBuffer()));
        }

        [Test]
        public void PooledByteBuffer_ResetClearsWrittenBytes()
        {
            var pooled = new PooledByteBuffer(1);
            pooled.AddByte(1);
            pooled.AddByte(2);

            var first = pooled.ToArray();
            Assert.That(first.Length, Is.EqualTo(2));
            Assert.That(first[0], Is.EqualTo(1));
            Assert.That(first[1], Is.EqualTo(2));

            pooled.Reset();
            pooled.AddInt(0x0A0B0C0D);
            var second = pooled.ToArray();

            Assert.That(second.Length, Is.EqualTo(4));
            Assert.That(second[0], Is.EqualTo(0x0A));
            Assert.That(second[1], Is.EqualTo(0x0B));
            Assert.That(second[2], Is.EqualTo(0x0C));
            Assert.That(second[3], Is.EqualTo(0x0D));
        }

        [Test]
        public void PooledByteBuffer_ReplaceBufferWritesToExternalArray()
        {
            var external = new byte[4];
            var pooled = new PooledByteBuffer(1);

            pooled.ReplaceBuffer(external, ownsBuffer: false);
            pooled.AddInt(0x01020304);

            Assert.That(ReferenceEquals(external, pooled.GetBufferUnsafe()), Is.True);
            Assert.That(external[0], Is.EqualTo(0x01));
            Assert.That(external[1], Is.EqualTo(0x02));
            Assert.That(external[2], Is.EqualTo(0x03));
            Assert.That(external[3], Is.EqualTo(0x04));
        }

        [Test]
        public void PooledByteBuffer_AddNullBinaryWritesZeroLength()
        {
            var pooled = new PooledByteBuffer(1);
            pooled.AddBinary(null);
            var buffer = pooled.ToArray();

            Assert.That(buffer.Length, Is.EqualTo(4));
            Assert.That(buffer[0], Is.EqualTo(0));
            Assert.That(buffer[1], Is.EqualTo(0));
            Assert.That(buffer[2], Is.EqualTo(0));
            Assert.That(buffer[3], Is.EqualTo(0));
        }
    }
}
