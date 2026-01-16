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
using System.Collections.Generic;
using Apache.IoTDB.DataStructure;
using NUnit.Framework;

namespace Apache.IoTDB.Tests
{
    [TestFixture]
    public class PooledTabletTests
    {
        [Test]
        public void PooledTablet_BinaryMatchesLegacyTablet_WithNullsAndSparse()
        {
            var measurements = new List<string> { "s1", "s2", "s3", "s4", "s5", "s6" };
            var dataTypes = new List<TSDataType>
            {
                TSDataType.INT64,
                TSDataType.BOOLEAN,
                TSDataType.DOUBLE,
                TSDataType.TEXT,
                TSDataType.DATE,
                TSDataType.BLOB
            };
            var timestamps = new List<long> { 1, 2, 3 };
            var values = new List<List<object>>
            {
                new() { 1L, true, 1.1, "a", new DateTime(2020, 1, 1), new byte[] { 1, 2 } },
                new() { 2L, null, 2.2, null, new DateTime(2020, 1, 2), null },
                new() { 3L, false, 3.3, "c", null, new byte[] { } }
            };

            var legacy = new Tablet("root.sg.d1", measurements, dataTypes, values, timestamps);

            var pooled = new PooledTablet("root.sg.d1", measurements, dataTypes, rowCapacity: 3);
            pooled.AddRow(1, new List<object> { 1L, true, 1.1, "a", new DateTime(2020, 1, 1), new byte[] { 1, 2 } });
            pooled.AddRow(2, new Dictionary<int, object>
            {
                { 0, 2L },
                { 2, 2.2 },
                { 4, new DateTime(2020, 1, 2) }
            });
            pooled.BeginRow(3);
            pooled.SetInt64(0, 3L);
            pooled.SetBoolean(1, false);
            pooled.SetDouble(2, 3.3);
            pooled.SetText(3, "c");
            pooled.SetNull(4);
            pooled.SetBlob(5, new byte[] { });
            pooled.EndRow();

            Assert.That(pooled.RowNumber, Is.EqualTo(legacy.RowNumber));
            Assert.That(pooled.GetBinaryTimestamps(), Is.EqualTo(legacy.GetBinaryTimestamps()));
            Assert.That(pooled.GetBinaryValues(), Is.EqualTo(legacy.GetBinaryValues()));
        }

        [Test]
        public void PooledTablet_ResetClearsNullBitmaps()
        {
            var measurements = new List<string> { "s1", "s2" };
            var dataTypes = new List<TSDataType> { TSDataType.INT32, TSDataType.TEXT };
            var pooled = new PooledTablet("root.sg.d2", measurements, dataTypes, rowCapacity: 2);

            pooled.AddRow(1, new List<object> { 10, null });
            pooled.GetBinaryValues();

            pooled.Reset();
            pooled.AddRow(2, new List<object> { 20, "ok" });
            pooled.AddRow(3, new List<object> { 30, "fine" });

            var legacyValues = new List<List<object>>
            {
                new() { 20, "ok" },
                new() { 30, "fine" }
            };
            var legacyTimestamps = new List<long> { 2, 3 };
            var legacy = new Tablet("root.sg.d2", measurements, dataTypes, legacyValues, legacyTimestamps);

            Assert.That(pooled.RowNumber, Is.EqualTo(legacy.RowNumber));
            Assert.That(pooled.GetBinaryTimestamps(), Is.EqualTo(legacy.GetBinaryTimestamps()));
            Assert.That(pooled.GetBinaryValues(), Is.EqualTo(legacy.GetBinaryValues()));
        }

        [Test]
        public void PooledTablet_BeginEndRow_MarksMissingColumnsAsNull()
        {
            var measurements = new List<string> { "s1", "s2" };
            var dataTypes = new List<TSDataType> { TSDataType.INT32, TSDataType.TEXT };
            var pooled = new PooledTablet("root.sg.d3", measurements, dataTypes, rowCapacity: 1);

            pooled.BeginRow(1);
            pooled.SetInt32(0, 42);
            pooled.EndRow();

            var legacyValues = new List<List<object>>
            {
                new() { 42, null }
            };
            var legacyTimestamps = new List<long> { 1 };
            var legacy = new Tablet("root.sg.d3", measurements, dataTypes, legacyValues, legacyTimestamps);

            Assert.That(pooled.GetBinaryValues(), Is.EqualTo(legacy.GetBinaryValues()));
        }

        [Test]
        public void PooledTablet_EnforcesSortedTimestamps()
        {
            var measurements = new List<string> { "s1" };
            var dataTypes = new List<TSDataType> { TSDataType.INT64 };
            var pooled = new PooledTablet("root.sg.d4", measurements, dataTypes, rowCapacity: 2);

            pooled.AddRow(2, new List<object> { 10L });
            Assert.Throws<Exception>(() => pooled.AddRow(1, new List<object> { 11L }));
        }

        [Test]
        public void PooledTablet_ResizePreservesNullBitMaps()
        {
            var measurements = new List<string> { "s1", "s2" };
            var dataTypes = new List<TSDataType> { TSDataType.INT32, TSDataType.TEXT };
            var pooled = new PooledTablet("root.sg.d5", measurements, dataTypes, rowCapacity: 1);

            pooled.AddRow(1, new List<object> { null, "a" });
            pooled.AddRow(2, new List<object> { 7, "b" });

            var legacyValues = new List<List<object>>
            {
                new() { null, "a" },
                new() { 7, "b" }
            };
            var legacyTimestamps = new List<long> { 1, 2 };
            var legacy = new Tablet("root.sg.d5", measurements, dataTypes, legacyValues, legacyTimestamps);

            Assert.That(pooled.GetBinaryValues(), Is.EqualTo(legacy.GetBinaryValues()));
        }

        [Test]
        public void PooledTablet_BindColumns_UsesExternalArraysAndBitmaps()
        {
            var measurements = new List<string> { "s1", "s2" };
            var dataTypes = new List<TSDataType> { TSDataType.INT32, TSDataType.TEXT };
            var pooled = new PooledTablet("root.sg.d6", measurements, dataTypes);

            var timestamps = new long[] { 10, 11 };
            var col1 = new int[] { 1, 2 };
            var col2 = new string[] { "x", "y" };
            var bitmaps = new BitMap[2];
            bitmaps[1] = new BitMap(2);
            bitmaps[1].mark(1);

            pooled.BindColumns(timestamps, new Array[] { col1, col2 }, 2, bitmaps);

            var legacyValues = new List<List<object>>
            {
                new() { 1, "x" },
                new() { 2, null }
            };
            var legacyTimestamps = new List<long> { 10, 11 };
            var legacy = new Tablet("root.sg.d6", measurements, dataTypes, legacyValues, legacyTimestamps);

            Assert.That(pooled.GetBinaryValues(), Is.EqualTo(legacy.GetBinaryValues()));
            Assert.That(pooled.GetBinaryTimestamps(), Is.EqualTo(legacy.GetBinaryTimestamps()));
        }
    }
}
