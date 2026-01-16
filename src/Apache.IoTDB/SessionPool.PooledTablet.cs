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

using System.Collections.Generic;
using System.Threading.Tasks;
using Apache.IoTDB.DataStructure;
using Microsoft.Extensions.Logging;

namespace Apache.IoTDB
{
    public partial class SessionPool
    {
        public TSInsertTabletReq GenInsertTabletReq(PooledTablet tablet, long sessionId)
        {
            return new TSInsertTabletReq(
                sessionId,
                tablet.InsertTargetName,
                tablet.Measurements,
                tablet.GetBinaryValues(),
                tablet.GetBinaryTimestamps(),
                tablet.GetDataTypes(),
                tablet.RowNumber);
        }

        public async Task<int> InsertTabletAsync(PooledTablet tablet)
        {
            return await ExecuteClientOperationAsync<int>(
                async client =>
                {
                    var req = GenInsertTabletReq(tablet, client.SessionId);

                    var status = await client.ServiceClient.insertTabletAsync(req);

                    if (_debugMode)
                    {
                        _logger.LogInformation("insert one tablet to device {0}, server message: {1}", tablet.InsertTargetName, status.Message);
                    }

                    return _utilFunctions.VerifySuccess(status);
                },
                errMsg: "Error occurs when inserting tablet"
            );
        }

        public async Task<int> InsertAlignedTabletAsync(PooledTablet tablet)
        {
            return await ExecuteClientOperationAsync<int>(
                async client =>
                {
                    var req = GenInsertTabletReq(tablet, client.SessionId);
                    req.IsAligned = true;

                    var status = await client.ServiceClient.insertTabletAsync(req);

                    if (_debugMode)
                    {
                        _logger.LogInformation("insert one aligned tablet to device {0}, server message: {1}", tablet.InsertTargetName, status.Message);
                    }

                    return _utilFunctions.VerifySuccess(status);
                },
                errMsg: "Error occurs when inserting aligned tablet"
            );
        }

        protected internal async Task<int> InsertRelationalTabletAsync(PooledTablet tablet)
        {
            return await ExecuteClientOperationAsync<int>(
                async client =>
                {
                    var req = GenInsertTabletReq(tablet, client.SessionId);
                    req.ColumnCategories = tablet.GetColumnColumnCategories();
                    req.WriteToTable = true;

                    var status = await client.ServiceClient.insertTabletAsync(req);

                    if (_debugMode)
                    {
                        _logger.LogInformation("insert one tablet to table {0}, server message: {1}", tablet.InsertTargetName, status.Message);
                    }

                    return _utilFunctions.VerifySuccess(status);
                },
                errMsg: "Error occurs when inserting tablet"
            );
        }

        public TSInsertTabletsReq GenInsertTabletsReq(List<PooledTablet> tabletLst, long sessionId)
        {
            var deviceIdLst = new List<string>();
            var measurementsLst = new List<List<string>>();
            var valuesLst = new List<byte[]>();
            var timestampsLst = new List<byte[]>();
            var typeLst = new List<List<int>>();
            var sizeLst = new List<int>();

            foreach (var tablet in tabletLst)
            {
                deviceIdLst.Add(tablet.InsertTargetName);
                measurementsLst.Add(tablet.Measurements);
                valuesLst.Add(tablet.GetBinaryValues());
                timestampsLst.Add(tablet.GetBinaryTimestamps());
                typeLst.Add(tablet.GetDataTypes());
                sizeLst.Add(tablet.RowNumber);
            }

            return new TSInsertTabletsReq(
                sessionId,
                deviceIdLst,
                measurementsLst,
                valuesLst,
                timestampsLst,
                typeLst,
                sizeLst);
        }

        public async Task<int> InsertTabletsAsync(List<PooledTablet> tabletLst)
        {
            return await ExecuteClientOperationAsync<int>(
                async client =>
                {
                    var req = GenInsertTabletsReq(tabletLst, client.SessionId);

                    var status = await client.ServiceClient.insertTabletsAsync(req);

                    if (_debugMode)
                    {
                        _logger.LogInformation("insert multiple tablets, server message: {0}", status.Message);
                    }

                    return _utilFunctions.VerifySuccess(status);
                },
                errMsg: "Error occurs when inserting tablets"
            );
        }

        public async Task<int> InsertAlignedTabletsAsync(List<PooledTablet> tabletLst)
        {
            return await ExecuteClientOperationAsync<int>(
                async client =>
                {
                    var req = GenInsertTabletsReq(tabletLst, client.SessionId);
                    req.IsAligned = true;

                    var status = await client.ServiceClient.insertTabletsAsync(req);

                    if (_debugMode)
                    {
                        _logger.LogInformation("insert multiple aligned tablets, server message: {0}", status.Message);
                    }

                    return _utilFunctions.VerifySuccess(status);
                },
                errMsg: "Error occurs when inserting aligned tablets"
            );
        }
    }
}
