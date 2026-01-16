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

namespace Apache.IoTDB.DataStructure
{
    public sealed class PooledTablet
    {
        private const int EmptyDateInt = 10000101;
        private const int DefaultBufferCapacity = 1024;
        private readonly PooledByteBuffer _valuesBuffer;
        private readonly PooledByteBuffer _timestampsBuffer;

        private long[] _timestamps;
        private Array[] _columns;
        private BitMap[] _bitMaps;
        private bool[] _columnHasNull;
        private bool _hasAnyNull;
        private bool[] _rowHasValue;
        private bool _hasActiveRow;
        private int _activeRowIndex;
        private int _rowCount;
        private long _lastTimestamp;
        private bool _enforceSorted;

        public string InsertTargetName { get; private set; }
        public List<string> Measurements { get; private set; }
        public List<TSDataType> DataTypes { get; private set; }
        public List<ColumnCategory> ColumnCategories { get; private set; }
        public BitMap[] BitMaps => _bitMaps;
        public int RowNumber => _rowCount;
        public int ColNumber { get; private set; }

        public PooledTablet(
            string deviceId,
            List<string> measurements,
            List<TSDataType> dataTypes,
            int rowCapacity = 0,
            bool enforceSorted = true,
            int valuesBufferCapacity = DefaultBufferCapacity,
            int timestampsBufferCapacity = DefaultBufferCapacity)
        {
            _valuesBuffer = new PooledByteBuffer(valuesBufferCapacity);
            _timestampsBuffer = new PooledByteBuffer(timestampsBufferCapacity);
            BindSchemaInternal(deviceId, measurements, dataTypes, null, rowCapacity, enforceSorted);
        }

        public PooledTablet(
            string tableName,
            List<string> columnNames,
            List<ColumnCategory> columnCategories,
            List<TSDataType> dataTypes,
            int rowCapacity = 0,
            bool enforceSorted = true,
            int valuesBufferCapacity = DefaultBufferCapacity,
            int timestampsBufferCapacity = DefaultBufferCapacity)
        {
            _valuesBuffer = new PooledByteBuffer(valuesBufferCapacity);
            _timestampsBuffer = new PooledByteBuffer(timestampsBufferCapacity);
            BindSchemaInternal(tableName, columnNames, dataTypes, columnCategories, rowCapacity, enforceSorted);
        }

        public void BindSchema(
            string insertTargetName,
            List<string> measurements,
            List<TSDataType> dataTypes,
            int rowCapacity = 0,
            bool enforceSorted = true)
        {
            BindSchemaInternal(insertTargetName, measurements, dataTypes, null, rowCapacity, enforceSorted);
        }

        public void BindSchema(
            string tableName,
            List<string> columnNames,
            List<ColumnCategory> columnCategories,
            List<TSDataType> dataTypes,
            int rowCapacity = 0,
            bool enforceSorted = true)
        {
            BindSchemaInternal(tableName, columnNames, dataTypes, columnCategories, rowCapacity, enforceSorted);
        }

        public void BindValuesBuffer(byte[] buffer, bool ownsBuffer = false)
        {
            _valuesBuffer.ReplaceBuffer(buffer, ownsBuffer);
        }

        public void BindTimestampsBuffer(byte[] buffer, bool ownsBuffer = false)
        {
            _timestampsBuffer.ReplaceBuffer(buffer, ownsBuffer);
        }

        public void Reset(int rowCapacity = -1)
        {
            _rowCount = 0;
            _lastTimestamp = long.MinValue;
            _hasActiveRow = false;
            _activeRowIndex = -1;
            _hasAnyNull = false;
            Array.Clear(_columnHasNull, 0, _columnHasNull.Length);

            if (_bitMaps != null)
            {
                foreach (var bitmap in _bitMaps)
                {
                    bitmap?.reset();
                }
            }

            if (rowCapacity >= 0)
            {
                EnsureCapacity(rowCapacity);
            }
        }

        public void EnsureCapacity(int rowCapacity)
        {
            if (rowCapacity <= _timestamps.Length)
            {
                return;
            }

            var newCapacity = _timestamps.Length == 0
                ? Math.Max(1, rowCapacity)
                : Math.Max(rowCapacity, _timestamps.Length * 2);

            Array.Resize(ref _timestamps, newCapacity);

            for (int i = 0; i < ColNumber; i++)
            {
                ResizeColumnArray(i, newCapacity);
            }

            ResizeBitMaps(newCapacity);
        }

        public int BeginRow(long timestamp)
        {
            if (_hasActiveRow)
            {
                throw new InvalidOperationException("The previous row has not been completed.");
            }

            EnsureCapacity(_rowCount + 1);
            CheckTimestampOrder(timestamp);

            var rowIndex = _rowCount++;
            _timestamps[rowIndex] = timestamp;
            _activeRowIndex = rowIndex;
            _hasActiveRow = true;
            Array.Clear(_rowHasValue, 0, _rowHasValue.Length);
            return rowIndex;
        }

        public void EndRow()
        {
            if (!_hasActiveRow)
            {
                throw new InvalidOperationException("No active row to finalize.");
            }

            for (int i = 0; i < ColNumber; i++)
            {
                if (!_rowHasValue[i])
                {
                    MarkNull(_activeRowIndex, i);
                }
            }

            _hasActiveRow = false;
            _activeRowIndex = -1;
        }

        public int AddRow(long timestamp, IReadOnlyList<object> values)
        {
            if (values == null)
            {
                throw new ArgumentNullException(nameof(values));
            }

            if (values.Count != ColNumber)
            {
                throw new ArgumentException("Values count does not match column count.", nameof(values));
            }

            EnsureCapacity(_rowCount + 1);
            CheckTimestampOrder(timestamp);

            var rowIndex = _rowCount++;
            _timestamps[rowIndex] = timestamp;

            for (int i = 0; i < ColNumber; i++)
            {
                SetValueInternal(rowIndex, i, values[i]);
            }

            return rowIndex;
        }

        public int AddRow(long timestamp, IDictionary<int, object> sparseValues)
        {
            if (sparseValues == null)
            {
                throw new ArgumentNullException(nameof(sparseValues));
            }

            EnsureCapacity(_rowCount + 1);
            CheckTimestampOrder(timestamp);

            var rowIndex = _rowCount++;
            _timestamps[rowIndex] = timestamp;

            for (int i = 0; i < ColNumber; i++)
            {
                if (sparseValues.TryGetValue(i, out var value))
                {
                    SetValueInternal(rowIndex, i, value);
                }
                else
                {
                    MarkNull(rowIndex, i);
                }
            }

            return rowIndex;
        }

        public void BindValues(IReadOnlyList<long> timestamps, IReadOnlyList<IReadOnlyList<object>> values)
        {
            if (timestamps == null)
            {
                throw new ArgumentNullException(nameof(timestamps));
            }

            if (values == null)
            {
                throw new ArgumentNullException(nameof(values));
            }

            if (timestamps.Count != values.Count)
            {
                throw new ArgumentException("Timestamps count does not match values count.");
            }

            Reset(timestamps.Count);
            EnsureCapacity(timestamps.Count);

            for (int row = 0; row < timestamps.Count; row++)
            {
                AddRow(timestamps[row], values[row]);
            }
        }

        public void BindColumns(long[] timestamps, Array[] columns, int rowCount, BitMap[] bitMaps = null)
        {
            if (timestamps == null)
            {
                throw new ArgumentNullException(nameof(timestamps));
            }

            if (columns == null)
            {
                throw new ArgumentNullException(nameof(columns));
            }

            if (columns.Length != ColNumber)
            {
                throw new ArgumentException("Column count does not match schema.");
            }

            if (rowCount < 0 || rowCount > timestamps.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(rowCount));
            }

            for (int i = 0; i < ColNumber; i++)
            {
                ValidateExternalColumn(i, columns[i], rowCount);
            }

            _timestamps = timestamps;
            _columns = columns;
            _rowCount = rowCount;
            _lastTimestamp = rowCount > 0 ? timestamps[rowCount - 1] : long.MinValue;
            _hasActiveRow = false;
            _activeRowIndex = -1;
            _bitMaps = bitMaps;
            _hasAnyNull = false;
            Array.Clear(_columnHasNull, 0, _columnHasNull.Length);

            if (bitMaps != null)
            {
                for (int i = 0; i < bitMaps.Length && i < ColNumber; i++)
                {
                    if (bitMaps[i] != null && !bitMaps[i].isAllUnmarked())
                    {
                        _columnHasNull[i] = true;
                        _hasAnyNull = true;
                    }
                }
            }
        }

        public void SetValue(int rowIndex, int columnIndex, object value)
        {
            EnsureValidCell(rowIndex, columnIndex);
            SetValueInternal(rowIndex, columnIndex, value);
        }

        public void SetValue(int columnIndex, object value)
        {
            EnsureActiveRow();
            SetValueInternal(_activeRowIndex, columnIndex, value);
            _rowHasValue[columnIndex] = true;
        }

        public void SetNull(int rowIndex, int columnIndex)
        {
            EnsureValidCell(rowIndex, columnIndex);
            MarkNull(rowIndex, columnIndex);
        }

        public void SetNull(int columnIndex)
        {
            EnsureActiveRow();
            MarkNull(_activeRowIndex, columnIndex);
            _rowHasValue[columnIndex] = true;
        }

        public void SetBoolean(int rowIndex, int columnIndex, bool value)
        {
            EnsureValidCell(rowIndex, columnIndex);
            EnsureColumnType(columnIndex, TSDataType.BOOLEAN);
            ((bool[])_columns[columnIndex])[rowIndex] = value;
            UnmarkNull(rowIndex, columnIndex);
        }

        public void SetBoolean(int columnIndex, bool value)
        {
            EnsureActiveRow();
            SetBoolean(_activeRowIndex, columnIndex, value);
            _rowHasValue[columnIndex] = true;
        }

        public void SetInt32(int rowIndex, int columnIndex, int value)
        {
            EnsureValidCell(rowIndex, columnIndex);
            EnsureColumnType(columnIndex, TSDataType.INT32);
            ((int[])_columns[columnIndex])[rowIndex] = value;
            UnmarkNull(rowIndex, columnIndex);
        }

        public void SetInt32(int columnIndex, int value)
        {
            EnsureActiveRow();
            SetInt32(_activeRowIndex, columnIndex, value);
            _rowHasValue[columnIndex] = true;
        }

        public void SetInt64(int rowIndex, int columnIndex, long value)
        {
            EnsureValidCell(rowIndex, columnIndex);
            EnsureColumnType(columnIndex, TSDataType.INT64, TSDataType.TIMESTAMP);
            ((long[])_columns[columnIndex])[rowIndex] = value;
            UnmarkNull(rowIndex, columnIndex);
        }

        public void SetInt64(int columnIndex, long value)
        {
            EnsureActiveRow();
            SetInt64(_activeRowIndex, columnIndex, value);
            _rowHasValue[columnIndex] = true;
        }

        public void SetFloat(int rowIndex, int columnIndex, float value)
        {
            EnsureValidCell(rowIndex, columnIndex);
            EnsureColumnType(columnIndex, TSDataType.FLOAT);
            ((float[])_columns[columnIndex])[rowIndex] = value;
            UnmarkNull(rowIndex, columnIndex);
        }

        public void SetFloat(int columnIndex, float value)
        {
            EnsureActiveRow();
            SetFloat(_activeRowIndex, columnIndex, value);
            _rowHasValue[columnIndex] = true;
        }

        public void SetDouble(int rowIndex, int columnIndex, double value)
        {
            EnsureValidCell(rowIndex, columnIndex);
            EnsureColumnType(columnIndex, TSDataType.DOUBLE);
            ((double[])_columns[columnIndex])[rowIndex] = value;
            UnmarkNull(rowIndex, columnIndex);
        }

        public void SetDouble(int columnIndex, double value)
        {
            EnsureActiveRow();
            SetDouble(_activeRowIndex, columnIndex, value);
            _rowHasValue[columnIndex] = true;
        }

        public void SetText(int rowIndex, int columnIndex, string value)
        {
            EnsureValidCell(rowIndex, columnIndex);
            EnsureColumnType(columnIndex, TSDataType.TEXT, TSDataType.STRING);
            ((string[])_columns[columnIndex])[rowIndex] = value ?? string.Empty;
            UnmarkNull(rowIndex, columnIndex);
        }

        public void SetText(int columnIndex, string value)
        {
            EnsureActiveRow();
            SetText(_activeRowIndex, columnIndex, value);
            _rowHasValue[columnIndex] = true;
        }

        public void SetBlob(int rowIndex, int columnIndex, byte[] value)
        {
            EnsureValidCell(rowIndex, columnIndex);
            EnsureColumnType(columnIndex, TSDataType.BLOB);
            ((byte[][])_columns[columnIndex])[rowIndex] = value ?? Array.Empty<byte>();
            UnmarkNull(rowIndex, columnIndex);
        }

        public void SetBlob(int columnIndex, byte[] value)
        {
            EnsureActiveRow();
            SetBlob(_activeRowIndex, columnIndex, value);
            _rowHasValue[columnIndex] = true;
        }

        public void SetDate(int rowIndex, int columnIndex, DateTime value)
        {
            EnsureValidCell(rowIndex, columnIndex);
            EnsureColumnType(columnIndex, TSDataType.DATE);
            ((int[])_columns[columnIndex])[rowIndex] = Utils.ParseDateToInt(value);
            UnmarkNull(rowIndex, columnIndex);
        }

        public void SetDate(int columnIndex, DateTime value)
        {
            EnsureActiveRow();
            SetDate(_activeRowIndex, columnIndex, value);
            _rowHasValue[columnIndex] = true;
        }

        public byte[] GetBinaryTimestamps()
        {
            _timestampsBuffer.Reset();
            _timestampsBuffer.EnsureCapacity(_rowCount * 8);

            for (int i = 0; i < _rowCount; i++)
            {
                _timestampsBuffer.AddLong(_timestamps[i]);
            }

            return _timestampsBuffer.ToArray();
        }

        public List<int> GetDataTypes()
        {
            return DataTypes.ConvertAll(x => (int)x);
        }

        public List<sbyte> GetColumnColumnCategories()
        {
            return ColumnCategories.ConvertAll(x => (sbyte)x);
        }

        public byte[] GetBinaryValues()
        {
            var estimateSize = EstimateBufferSize();
            _valuesBuffer.Reset();
            _valuesBuffer.EnsureCapacity(estimateSize);

            for (int i = 0; i < ColNumber; i++)
            {
                var dataType = DataTypes[i];
                var hasNull = _columnHasNull[i];
                var bitmap = _bitMaps != null ? _bitMaps[i] : null;

                switch (dataType)
                {
                    case TSDataType.BOOLEAN:
                    {
                        var column = (bool[])_columns[i];
                        if (!hasNull)
                        {
                            for (int j = 0; j < _rowCount; j++)
                            {
                                _valuesBuffer.AddBool(column[j]);
                            }
                        }
                        else
                        {
                            for (int j = 0; j < _rowCount; j++)
                            {
                                var value = bitmap != null && bitmap.isMarked(j) ? false : column[j];
                                _valuesBuffer.AddBool(value);
                            }
                        }
                        break;
                    }
                    case TSDataType.INT32:
                    {
                        var column = (int[])_columns[i];
                        if (!hasNull)
                        {
                            for (int j = 0; j < _rowCount; j++)
                            {
                                _valuesBuffer.AddInt(column[j]);
                            }
                        }
                        else
                        {
                            for (int j = 0; j < _rowCount; j++)
                            {
                                var value = bitmap != null && bitmap.isMarked(j) ? int.MinValue : column[j];
                                _valuesBuffer.AddInt(value);
                            }
                        }
                        break;
                    }
                    case TSDataType.INT64:
                    case TSDataType.TIMESTAMP:
                    {
                        var column = (long[])_columns[i];
                        if (!hasNull)
                        {
                            for (int j = 0; j < _rowCount; j++)
                            {
                                _valuesBuffer.AddLong(column[j]);
                            }
                        }
                        else
                        {
                            for (int j = 0; j < _rowCount; j++)
                            {
                                var value = bitmap != null && bitmap.isMarked(j) ? long.MinValue : column[j];
                                _valuesBuffer.AddLong(value);
                            }
                        }
                        break;
                    }
                    case TSDataType.FLOAT:
                    {
                        var column = (float[])_columns[i];
                        if (!hasNull)
                        {
                            for (int j = 0; j < _rowCount; j++)
                            {
                                _valuesBuffer.AddFloat(column[j]);
                            }
                        }
                        else
                        {
                            for (int j = 0; j < _rowCount; j++)
                            {
                                var value = bitmap != null && bitmap.isMarked(j) ? float.MinValue : column[j];
                                _valuesBuffer.AddFloat(value);
                            }
                        }
                        break;
                    }
                    case TSDataType.DOUBLE:
                    {
                        var column = (double[])_columns[i];
                        if (!hasNull)
                        {
                            for (int j = 0; j < _rowCount; j++)
                            {
                                _valuesBuffer.AddDouble(column[j]);
                            }
                        }
                        else
                        {
                            for (int j = 0; j < _rowCount; j++)
                            {
                                var value = bitmap != null && bitmap.isMarked(j) ? double.MinValue : column[j];
                                _valuesBuffer.AddDouble(value);
                            }
                        }
                        break;
                    }
                    case TSDataType.TEXT:
                    case TSDataType.STRING:
                    {
                        var column = (string[])_columns[i];
                        if (!hasNull)
                        {
                            for (int j = 0; j < _rowCount; j++)
                            {
                                _valuesBuffer.AddStr(column[j] ?? string.Empty);
                            }
                        }
                        else
                        {
                            for (int j = 0; j < _rowCount; j++)
                            {
                                var value = bitmap != null && bitmap.isMarked(j) ? string.Empty : column[j];
                                _valuesBuffer.AddStr(value ?? string.Empty);
                            }
                        }
                        break;
                    }
                    case TSDataType.DATE:
                    {
                        var column = (int[])_columns[i];
                        if (!hasNull)
                        {
                            for (int j = 0; j < _rowCount; j++)
                            {
                                _valuesBuffer.AddInt(column[j]);
                            }
                        }
                        else
                        {
                            for (int j = 0; j < _rowCount; j++)
                            {
                                var value = bitmap != null && bitmap.isMarked(j) ? EmptyDateInt : column[j];
                                _valuesBuffer.AddInt(value);
                            }
                        }
                        break;
                    }
                    case TSDataType.BLOB:
                    {
                        var column = (byte[][])_columns[i];
                        if (!hasNull)
                        {
                            for (int j = 0; j < _rowCount; j++)
                            {
                                _valuesBuffer.AddBinary(column[j] ?? Array.Empty<byte>());
                            }
                        }
                        else
                        {
                            for (int j = 0; j < _rowCount; j++)
                            {
                                var value = bitmap != null && bitmap.isMarked(j) ? Array.Empty<byte>() : column[j];
                                _valuesBuffer.AddBinary(value ?? Array.Empty<byte>());
                            }
                        }
                        break;
                    }
                    default:
                        throw new Exception($"Unsupported data type {dataType}", null);
                }
            }

            if (_hasAnyNull && _bitMaps != null)
            {
                for (int i = 0; i < ColNumber; i++)
                {
                    var columnHasNull = _columnHasNull[i];
                    _valuesBuffer.AddBool(columnHasNull);
                    if (columnHasNull)
                    {
                        var bitmap = _bitMaps[i];
                        var bytes = bitmap.getByteArray();
                        var bytesToWrite = _rowCount / 8 + 1;
                        for (int j = 0; j < bytesToWrite; j++)
                        {
                            _valuesBuffer.AddByte(bytes[j]);
                        }
                    }
                }
            }

            return _valuesBuffer.ToArray();
        }

        private void BindSchemaInternal(
            string insertTargetName,
            List<string> measurements,
            List<TSDataType> dataTypes,
            List<ColumnCategory> columnCategories,
            int rowCapacity,
            bool enforceSorted)
        {
            if (measurements == null)
            {
                throw new ArgumentNullException(nameof(measurements));
            }

            if (dataTypes == null)
            {
                throw new ArgumentNullException(nameof(dataTypes));
            }

            if (measurements.Count != dataTypes.Count)
            {
                throw new Exception(
                    $"Input error. Measurements.Count({measurements.Count}) does not equal to DataTypes.Count({dataTypes.Count}).",
                    null);
            }

            if (columnCategories != null && measurements.Count != columnCategories.Count)
            {
                throw new Exception(
                    $"Input error. Measurements.Count({measurements.Count}) does not equal to ColumnCategories.Count({columnCategories.Count}).",
                    null);
            }

            InsertTargetName = insertTargetName;
            Measurements = measurements;
            DataTypes = dataTypes;
            ColumnCategories = columnCategories;
            ColNumber = measurements.Count;
            _enforceSorted = enforceSorted;
            InitializeStorage(rowCapacity);
        }

        private void InitializeStorage(int rowCapacity)
        {
            _rowCount = 0;
            _lastTimestamp = long.MinValue;
            _hasActiveRow = false;
            _activeRowIndex = -1;
            _hasAnyNull = false;

            var capacity = Math.Max(0, rowCapacity);
            _timestamps = capacity == 0 ? Array.Empty<long>() : new long[capacity];
            _columns = new Array[ColNumber];
            for (int i = 0; i < ColNumber; i++)
            {
                _columns[i] = CreateColumnArray(DataTypes[i], capacity);
            }

            _rowHasValue = new bool[ColNumber];
            _bitMaps = null;
            _columnHasNull = new bool[ColNumber];
        }

        private Array CreateColumnArray(TSDataType dataType, int capacity)
        {
            switch (dataType)
            {
                case TSDataType.BOOLEAN:
                    return new bool[capacity];
                case TSDataType.INT32:
                case TSDataType.DATE:
                    return new int[capacity];
                case TSDataType.INT64:
                case TSDataType.TIMESTAMP:
                    return new long[capacity];
                case TSDataType.FLOAT:
                    return new float[capacity];
                case TSDataType.DOUBLE:
                    return new double[capacity];
                case TSDataType.TEXT:
                case TSDataType.STRING:
                    return new string[capacity];
                case TSDataType.BLOB:
                    return new byte[capacity][];
                default:
                    throw new Exception($"Unsupported data type {dataType}", null);
            }
        }

        private void ResizeColumnArray(int columnIndex, int newCapacity)
        {
            switch (DataTypes[columnIndex])
            {
                case TSDataType.BOOLEAN:
                {
                    var column = (bool[])_columns[columnIndex];
                    Array.Resize(ref column, newCapacity);
                    _columns[columnIndex] = column;
                    break;
                }
                case TSDataType.INT32:
                case TSDataType.DATE:
                {
                    var column = (int[])_columns[columnIndex];
                    Array.Resize(ref column, newCapacity);
                    _columns[columnIndex] = column;
                    break;
                }
                case TSDataType.INT64:
                case TSDataType.TIMESTAMP:
                {
                    var column = (long[])_columns[columnIndex];
                    Array.Resize(ref column, newCapacity);
                    _columns[columnIndex] = column;
                    break;
                }
                case TSDataType.FLOAT:
                {
                    var column = (float[])_columns[columnIndex];
                    Array.Resize(ref column, newCapacity);
                    _columns[columnIndex] = column;
                    break;
                }
                case TSDataType.DOUBLE:
                {
                    var column = (double[])_columns[columnIndex];
                    Array.Resize(ref column, newCapacity);
                    _columns[columnIndex] = column;
                    break;
                }
                case TSDataType.TEXT:
                case TSDataType.STRING:
                {
                    var column = (string[])_columns[columnIndex];
                    Array.Resize(ref column, newCapacity);
                    _columns[columnIndex] = column;
                    break;
                }
                case TSDataType.BLOB:
                {
                    var column = (byte[][])_columns[columnIndex];
                    Array.Resize(ref column, newCapacity);
                    _columns[columnIndex] = column;
                    break;
                }
                default:
                    throw new Exception($"Unsupported data type {DataTypes[columnIndex]}", null);
            }
        }

        private void ResizeBitMaps(int newCapacity)
        {
            if (_bitMaps == null)
            {
                return;
            }

            for (int i = 0; i < _bitMaps.Length; i++)
            {
                var bitmap = _bitMaps[i];
                if (bitmap == null)
                {
                    continue;
                }

                var oldBytes = bitmap.getByteArray();
                var newBytes = new byte[newCapacity / 8 + 1];
                Buffer.BlockCopy(oldBytes, 0, newBytes, 0, oldBytes.Length);
                _bitMaps[i] = new BitMap(newCapacity, newBytes);
            }
        }

        private void CheckTimestampOrder(long timestamp)
        {
            if (_enforceSorted && _rowCount > 0 && timestamp < _lastTimestamp)
            {
                throw new Exception("Timestamps are not in non-decreasing order.", null);
            }

            _lastTimestamp = timestamp;
        }

        private void EnsureActiveRow()
        {
            if (!_hasActiveRow)
            {
                throw new InvalidOperationException("No active row. Call BeginRow first.");
            }
        }

        private void EnsureValidCell(int rowIndex, int columnIndex)
        {
            if (rowIndex < 0 || rowIndex >= _rowCount)
            {
                throw new ArgumentOutOfRangeException(nameof(rowIndex));
            }

            if (columnIndex < 0 || columnIndex >= ColNumber)
            {
                throw new ArgumentOutOfRangeException(nameof(columnIndex));
            }
        }

        private void EnsureColumnType(int columnIndex, params TSDataType[] validTypes)
        {
            var dataType = DataTypes[columnIndex];
            foreach (var valid in validTypes)
            {
                if (dataType == valid)
                {
                    return;
                }
            }

            throw new ArgumentException($"Column {columnIndex} type is {dataType}.");
        }

        private void MarkNull(int rowIndex, int columnIndex)
        {
            if (_bitMaps == null)
            {
                _bitMaps = new BitMap[ColNumber];
            }

            var bitmap = _bitMaps[columnIndex];
            if (bitmap == null)
            {
                bitmap = new BitMap(_timestamps.Length);
                _bitMaps[columnIndex] = bitmap;
            }

            bitmap.mark(rowIndex);
            _columnHasNull[columnIndex] = true;
            _hasAnyNull = true;
        }

        private void UnmarkNull(int rowIndex, int columnIndex)
        {
            if (_bitMaps == null)
            {
                return;
            }

            var bitmap = _bitMaps[columnIndex];
            bitmap?.unmark(rowIndex);
        }

        private void SetValueInternal(int rowIndex, int columnIndex, object value)
        {
            if (value == null)
            {
                MarkNull(rowIndex, columnIndex);
                return;
            }

            switch (DataTypes[columnIndex])
            {
                case TSDataType.BOOLEAN:
                    SetBoolean(rowIndex, columnIndex, (bool)value);
                    break;
                case TSDataType.INT32:
                    SetInt32(rowIndex, columnIndex, (int)value);
                    break;
                case TSDataType.INT64:
                case TSDataType.TIMESTAMP:
                    SetInt64(rowIndex, columnIndex, (long)value);
                    break;
                case TSDataType.FLOAT:
                    SetFloat(rowIndex, columnIndex, (float)value);
                    break;
                case TSDataType.DOUBLE:
                    SetDouble(rowIndex, columnIndex, (double)value);
                    break;
                case TSDataType.TEXT:
                case TSDataType.STRING:
                    SetText(rowIndex, columnIndex, (string)value);
                    break;
                case TSDataType.DATE:
                    SetDate(rowIndex, columnIndex, (DateTime)value);
                    break;
                case TSDataType.BLOB:
                    SetBlob(rowIndex, columnIndex, (byte[])value);
                    break;
                default:
                    throw new Exception($"Unsupported data type {DataTypes[columnIndex]}", null);
            }
        }

        private void ValidateExternalColumn(int columnIndex, Array column, int rowCount)
        {
            if (column == null)
            {
                throw new ArgumentNullException(nameof(column));
            }

            if (column.Length < rowCount)
            {
                throw new ArgumentException($"Column {columnIndex} length is smaller than row count.");
            }

            switch (DataTypes[columnIndex])
            {
                case TSDataType.BOOLEAN:
                    if (column is not bool[]) throw new ArgumentException($"Column {columnIndex} type mismatch.");
                    break;
                case TSDataType.INT32:
                case TSDataType.DATE:
                    if (column is not int[]) throw new ArgumentException($"Column {columnIndex} type mismatch.");
                    break;
                case TSDataType.INT64:
                case TSDataType.TIMESTAMP:
                    if (column is not long[]) throw new ArgumentException($"Column {columnIndex} type mismatch.");
                    break;
                case TSDataType.FLOAT:
                    if (column is not float[]) throw new ArgumentException($"Column {columnIndex} type mismatch.");
                    break;
                case TSDataType.DOUBLE:
                    if (column is not double[]) throw new ArgumentException($"Column {columnIndex} type mismatch.");
                    break;
                case TSDataType.TEXT:
                case TSDataType.STRING:
                    if (column is not string[]) throw new ArgumentException($"Column {columnIndex} type mismatch.");
                    break;
                case TSDataType.BLOB:
                    if (column is not byte[][]) throw new ArgumentException($"Column {columnIndex} type mismatch.");
                    break;
                default:
                    throw new Exception($"Unsupported data type {DataTypes[columnIndex]}", null);
            }
        }

        private int EstimateBufferSize()
        {
            var estimateSize = 0;

            foreach (var dataType in DataTypes)
            {
                switch (dataType)
                {
                    case TSDataType.BOOLEAN:
                        estimateSize += 1;
                        break;
                    case TSDataType.INT32:
                    case TSDataType.DATE:
                        estimateSize += 4;
                        break;
                    case TSDataType.INT64:
                    case TSDataType.TIMESTAMP:
                        estimateSize += 8;
                        break;
                    case TSDataType.FLOAT:
                        estimateSize += 4;
                        break;
                    case TSDataType.DOUBLE:
                        estimateSize += 8;
                        break;
                    case TSDataType.TEXT:
                    case TSDataType.BLOB:
                    case TSDataType.STRING:
                        estimateSize += 8;
                        break;
                    default:
                        throw new Exception(
                            $"Input error. Data type {dataType} is not supported.",
                            null);
                }
            }

            estimateSize *= _rowCount;
            return estimateSize;
        }
    }
}
