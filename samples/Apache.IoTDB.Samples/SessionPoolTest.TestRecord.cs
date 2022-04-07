using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Apache.IoTDB.DataStructure;

namespace Apache.IoTDB.Samples
{
    public partial class SessionPoolTest
    {
        public async Task TestInsertAlignedRecordsOfOneDevice()
        {
            var session_pool = new SessionPool(host, port, pool_size);
            await session_pool.Open(false);
            if (debug) session_pool.OpenDebugMode();

            System.Diagnostics.Debug.Assert(session_pool.IsOpen());
            var status = 0;
            await session_pool.DeleteStorageGroupAsync(test_group_name);

            string prefixPath = string.Format("{0}.{1}", test_group_name, test_device);
            var measurement_lst = new List<string>()
            {
                test_measurements[1],
                test_measurements[2],
                test_measurements[3],
                test_measurements[4],
                test_measurements[5],
                test_measurements[6]
            };
            var data_type_lst = new List<TSDataType>()
            {
                TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.DOUBLE, TSDataType.FLOAT,
                TSDataType.TEXT
            };
            var encoding_lst = new List<TSEncoding>()
            {
                TSEncoding.PLAIN, TSEncoding.PLAIN, TSEncoding.PLAIN, TSEncoding.PLAIN, TSEncoding.PLAIN,
                TSEncoding.PLAIN
            };
            var compressor_lst = new List<Compressor>()
            {
                Compressor.SNAPPY, Compressor.SNAPPY, Compressor.SNAPPY, Compressor.SNAPPY, Compressor.SNAPPY,
                Compressor.SNAPPY
            };

            status = await session_pool.CreateAlignedTimeseriesAsync(prefixPath, measurement_lst, data_type_lst, encoding_lst,
                compressor_lst);
            System.Diagnostics.Debug.Assert(status == 0);

            var device_id = string.Format("{0}.{1}", test_group_name, test_device);
            var measurements_lst = new List<List<string>>() { };
            measurements_lst.Add(new List<string>() { test_measurements[1], test_measurements[2] });
            measurements_lst.Add(new List<string>()
            {
                test_measurements[1],
                test_measurements[2],
                test_measurements[3],
                test_measurements[4]
            });
            measurements_lst.Add(new List<string>()
            {
                test_measurements[1],
                test_measurements[2],
                test_measurements[3],
                test_measurements[4],
                test_measurements[5],
                test_measurements[6]
            });
            var values_lst = new List<List<object>>() { };
            values_lst.Add(new List<object>() { true, (int)123 });
            values_lst.Add(new List<object>() { true, (int)123, (long)456, (double)1.1 });
            values_lst.Add(new List<object>()
                {true, (int) 123, (long) 456, (double) 1.1, (float) 10001.1, "test_record"});
            var timestamp_lst = new List<long>() { 1, 2, 3 };
            var rowRecords = new List<RowRecord>() { };
            for (var i = 0; i < 3; i++)
            {
                var rowRecord = new RowRecord(timestamp_lst[i], values_lst[i], measurements_lst[i]);
                rowRecords.Add(rowRecord);
            }
            status = await session_pool.InsertAlignedRecordsOfOneDeviceAsync(device_id, rowRecords);
            System.Diagnostics.Debug.Assert(status == 0);
            var res = await session_pool.ExecuteQueryStatementAsync(
                "select * from " + string.Format("{0}.{1}", test_group_name, test_device) + " where time<10");
            res.ShowTableNames();
            while (res.HasNext()) Console.WriteLine(res.Next());

            await res.Close();
            rowRecords = new List<RowRecord>() { };
            var tasks = new List<Task<int>>();
            for (var timestamp = 4; timestamp <= fetch_size * processed_size; timestamp++)
            {
                rowRecords.Add(new RowRecord(timestamp, new List<object>() { true, (int)123 },
                    new List<string>() { test_measurements[1], test_measurements[2] }));
                if (timestamp % fetch_size == 0)
                {
                    tasks.Add(session_pool.InsertAlignedRecordsOfOneDeviceAsync(device_id, rowRecords));
                    rowRecords = new List<RowRecord>() { };
                }
            }

            Task.WaitAll(tasks.ToArray());
            Thread.Sleep(20);
            res = await session_pool.ExecuteQueryStatementAsync(
                "select * from " + string.Format("{0}.{1}", test_group_name, test_device));
            var res_count = 0;
            while (res.HasNext())
            {
                res.Next();
                res_count += 1;
            }

            await res.Close();
            Console.WriteLine(res_count + " " + fetch_size * processed_size);
            System.Diagnostics.Debug.Assert(res_count == fetch_size * processed_size);
            // status = await session_pool.DeleteStorageGroupAsync("root.TEST_CSHARP_CLIENT_GROUP_97209");
            System.Diagnostics.Debug.Assert(status == 0);
            await session_pool.Close();
            Console.WriteLine("TestInsertAlignedRecordsOfOneDevice Passed!");
        }
    }
}