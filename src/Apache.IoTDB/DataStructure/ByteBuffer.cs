using System;
using System.Linq;

namespace Apache.IoTDB.DataStructure
{
    public class ByteBuffer
    {
        private byte[] buffer;
        private int write_pos = 0, read_pos = 0;
        private int total_length;
        private bool is_little_endian;

        public ByteBuffer(byte[] buffer)
        {
            this.buffer = buffer;
            read_pos = 0;
            write_pos = buffer.Length;
            total_length = buffer.Length;
            is_little_endian = BitConverter.IsLittleEndian;
        }

        public ByteBuffer(int reserve = 1)
        {
            buffer = new byte[reserve];
            write_pos = 0;
            read_pos = 0;
            total_length = reserve;
            is_little_endian = BitConverter.IsLittleEndian;
        }

        public bool has_remaining()
        {
            return read_pos < write_pos;
        }

        // these for read
        public byte get_byte()
        {
            var byte_val = buffer[read_pos];
            read_pos += 1;
            return byte_val;
        }

        public bool get_bool()
        {
            var bool_value = BitConverter.ToBoolean(buffer, read_pos);
            read_pos += 1;
            return bool_value;
        }

        public int get_int()
        {
            var int_buff = buffer[read_pos..(read_pos + 4)];
            if (is_little_endian) int_buff = int_buff.Reverse().ToArray();

            var int_value = BitConverter.ToInt32(int_buff);
            read_pos += 4;
            return int_value;
        }

        public long get_long()
        {
            var long_buff = buffer[read_pos..(read_pos + 8)];
            if (is_little_endian) long_buff = long_buff.Reverse().ToArray();

            var long_value = BitConverter.ToInt64(long_buff);
            read_pos += 8;
            return long_value;
        }

        public float get_float()
        {
            var float_buff = buffer[read_pos..(read_pos + 4)];
            if (is_little_endian) float_buff = float_buff.Reverse().ToArray();

            var float_value = BitConverter.ToSingle(float_buff);
            read_pos += 4;
            return float_value;
        }

        public double get_double()
        {
            var double_buff = buffer[read_pos..(read_pos + 8)];
            if (is_little_endian) double_buff = double_buff.Reverse().ToArray();

            var double_value = BitConverter.ToDouble(double_buff);
            read_pos += 8;
            return double_value;
        }

        public string get_str()
        {
            var length = get_int();
            var str_buff = buffer[read_pos..(read_pos + length)];
            var str_value = System.Text.Encoding.UTF8.GetString(str_buff);
            read_pos += length;
            return str_value;
        }

        public byte[] get_buffer()
        {
            return buffer[0..write_pos];
        }

        private int max(int a, int b)
        {
            if (a <= b) return b;

            return a;
        }

        private void extend_buffer(int space_need)
        {
            if (write_pos + space_need >= total_length)
            {
                total_length = max(space_need, total_length);
                var new_buffer = new byte[total_length * 2];
                buffer.CopyTo(new_buffer, 0);
                buffer = new_buffer;
                total_length = 2 * total_length;
            }
        }

        // these for write
        public void add_bool(bool value)
        {
            var bool_buffer = BitConverter.GetBytes(value);
            if (is_little_endian) bool_buffer = bool_buffer.Reverse().ToArray();

            extend_buffer(bool_buffer.Length);
            bool_buffer.CopyTo(buffer, write_pos);
            write_pos += bool_buffer.Length;
        }

        public void add_int(int value)
        {
            var int_buff = BitConverter.GetBytes(value);
            if (is_little_endian) int_buff = int_buff.Reverse().ToArray();

            extend_buffer(int_buff.Length);
            int_buff.CopyTo(buffer, write_pos);
            write_pos += int_buff.Length;
        }

        public void add_long(long value)
        {
            var long_buff = BitConverter.GetBytes(value);
            if (is_little_endian) long_buff = long_buff.Reverse().ToArray();

            extend_buffer(long_buff.Length);
            long_buff.CopyTo(buffer, write_pos);
            write_pos += long_buff.Length;
        }

        public void add_float(float value)
        {
            var float_buff = BitConverter.GetBytes(value);
            if (is_little_endian) float_buff = float_buff.Reverse().ToArray();

            extend_buffer(float_buff.Length);
            float_buff.CopyTo(buffer, write_pos);
            write_pos += float_buff.Length;
        }

        public void add_double(double value)
        {
            var double_buff = BitConverter.GetBytes(value);
            if (is_little_endian) double_buff = double_buff.Reverse().ToArray();

            extend_buffer(double_buff.Length);
            double_buff.CopyTo(buffer, write_pos);
            write_pos += double_buff.Length;
        }

        public void add_str(string value)
        {
            add_int(value.Length);
            var str_buf = System.Text.Encoding.UTF8.GetBytes(value);

            extend_buffer(str_buf.Length);
            str_buf.CopyTo(buffer, write_pos);
            write_pos += str_buf.Length;
        }

        public void add_char(char value)
        {
            var char_buf = BitConverter.GetBytes(value);
            if (is_little_endian) char_buf = char_buf.Reverse().ToArray();

            extend_buffer(char_buf.Length);
            char_buf.CopyTo(buffer, write_pos);
            write_pos += char_buf.Length;
        }
    }
}