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
using System.Threading.Tasks;
using Apache.IoTDB.DataStructure;

namespace Apache.IoTDB.Samples
{
    public partial class SessionPoolTest
    {
        public async Task TestInsertPooledTablet()
        {
            var session_pool = new SessionPool(host, port, poolSize);
            await session_pool.Open(false);
            if (debug) session_pool.OpenDebugMode();

            await session_pool.DeleteDatabaseAsync(testDatabaseName);
            var device_id = string.Format("{0}.{1}", testDatabaseName, testDevice);
            var measurement_lst = new List<string>
            {
                testMeasurements[1],
                testMeasurements[2],
                testMeasurements[3]
            };
            var datatype_lst = new List<TSDataType>
            {
                TSDataType.TEXT,
                TSDataType.BOOLEAN,
                TSDataType.INT32
            };

            var batchSize = 5;
            var tablet = new PooledTablet(device_id, measurement_lst, datatype_lst, rowCapacity: batchSize);

            for (int timestamp = 1; timestamp <= 12; timestamp++)
            {
                tablet.BeginRow(timestamp);
                tablet.SetText(0, "iotdb");
                if (timestamp % 2 == 0)
                {
                    tablet.SetBoolean(1, true);
                }
                tablet.SetInt32(2, timestamp);
                tablet.EndRow();

                if (tablet.RowNumber >= batchSize)
                {
                    await session_pool.InsertTabletAsync(tablet);
                    tablet.Reset();
                }
            }

            if (tablet.RowNumber > 0)
            {
                await session_pool.InsertTabletAsync(tablet);
                tablet.Reset();
            }

            await session_pool.DeleteDatabaseAsync(testDatabaseName);
            await session_pool.Close();
            Console.WriteLine("TestInsertPooledTablet Passed!");
        }
    }
}
