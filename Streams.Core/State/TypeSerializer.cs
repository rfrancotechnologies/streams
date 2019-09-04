using System.Text;

namespace Com.RFranco.Streams.State
{
    /// <summary>
    /// TypeSerializer interface to implement serialize / deserialize T instances
    /// </summary>
    /// <typeparam name="T">Type that represents the state value</typeparam>
    public interface TypeSerializer<T>
    {
        /// <summary>
        /// Deserialize an array of bytes to a T representation
        /// </summary>
        /// <param name="data">Array of bytes</param>
        /// <returns>T instance of the array of bytes provided</returns>
        T Deserialize(byte[] data);

        /// <summary>
        /// Serialize a T instance to array of bytes
        /// </summary>
        /// <param name="data">T instance</param>
        /// <returns>Array of bytes of T instance provided</returns>
        byte[] Serialize(T data);
    }

    /// <summary>
    /// Provides TypeSerializer instances of primitive types
    /// </summary>
    public static class Serializers
    {
        /// <summary>
        /// String TypeSerializer implementation instance
        /// </summary>
        /// <returns></returns>
        public static StringSerializer StringSerializer = new StringSerializer();

        /// <summary>
        /// Nullable Long TypeSerializer implementation instance
        /// </summary>
        /// <returns></returns>
        public static NullableLongSerializer NullableLongSerializer = new NullableLongSerializer();

        /// <summary>
        /// Long TypeSerializer implementation instance
        /// </summary>
        /// <returns></returns>
        public static LongSerializer LongSerializer = new LongSerializer();

        /// <summary>
        /// Nullable int TypeSerializer implementation instance
        /// </summary>
        /// <returns></returns>
        public static NullableIntSerializer NullableIntSerializer = new NullableIntSerializer();

        /// <summary>
        /// Int TypeSerializer implementation instance
        /// </summary>
        /// <returns></returns>
        public static IntSerializer IntSerializer = new IntSerializer();
    }

    /// <summary>
    /// String TypeSerializer implementation
    /// </summary>
    public class StringSerializer : TypeSerializer<string>
    {
        public string Deserialize(byte[] data)
        {
            return Encoding.UTF8.GetString(data, 0, data.Length);
        }

        public byte[] Serialize(string data)
        {
            return Encoding.UTF8.GetBytes(data);
        }
    }

    /// <summary>
    /// Nullable long TypeSerializer implementation
    /// </summary>
    public class NullableLongSerializer : TypeSerializer<long?>
    {
        public long? Deserialize(byte[] data)
        {
            var str = Encoding.UTF8.GetString(data, 0, data.Length);
            long value;
            if (long.TryParse(str, out value)) return value;
            return null;
        }

        public byte[] Serialize(long? data)
        {
            if (!data.HasValue) return new byte[0];
            else return Encoding.UTF8.GetBytes((data.Value.ToString()));
        }
    }

    /// <summary>
    /// Long TypeSerializer implementation
    /// </summary>
    public class LongSerializer : TypeSerializer<long>
    {
        public long Deserialize(byte[] data)
        {
            return (long.Parse(Encoding.UTF8.GetString(data, 0, data.Length)));
        }

        public byte[] Serialize(long data)
        {
            return Encoding.UTF8.GetBytes((data.ToString()));
        }
    }

    /// <summary>
    /// Nullable int TypeSerializer implementation
    /// </summary>
    public class NullableIntSerializer : TypeSerializer<int?>
    {
        public int? Deserialize(byte[] data)
        {
            var str = Encoding.UTF8.GetString(data, 0, data.Length);
            int value;
            if (int.TryParse(str, out value)) return value;
            return null;
        }

        public byte[] Serialize(int? data)
        {
            if (!data.HasValue) return new byte[0];
            else return Encoding.UTF8.GetBytes((data.Value.ToString()));
        }
    }

    /// <summary>
    /// Int TypeSerializer implementation
    /// </summary>
    public class IntSerializer : TypeSerializer<int>
    {
        public int Deserialize(byte[] data)
        {
            return (int.Parse(Encoding.UTF8.GetString(data, 0, data.Length)));
        }

        public byte[] Serialize(int data)
        {
            return Encoding.UTF8.GetBytes((data.ToString()));
        }
    }

}