using System;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

namespace Com.RFranco.Streams.State
{
    /// <summary>
    /// StateStorage definition
    /// </summary>    
    public abstract class StateStorage
    {
        /// <summary>
        /// Get value
        /// </summary>
        /// <returns></returns>
        public abstract object GetValue();

        /// <summary>
        /// Update value
        /// </summary>
        /// <param name="newValue"></param>
        public abstract void Update(object newValue);

        /// <summary>
        /// Close storage
        /// </summary>
        public abstract void Close();

        /// <summary>
        /// Clear value 
        /// </summary>
        public abstract void Clear();

        /// <summary>
        /// Serialize object (must be marked as Serializable or throw InvalidOperationException)
        /// </summary>
        /// <param name="obj">Object to be serialized</param>
        /// <returns>Array of bytes representation of the object</returns>
        protected byte[] Serialize(object obj)
        {
            if (obj == null)
                return null;
            try
            {
                BinaryFormatter bf = new BinaryFormatter();
                using (MemoryStream ms = new MemoryStream())
                {
                    bf.Serialize(ms, obj);
                    return ms.ToArray();
                }
            } catch (Exception e)
            {
                throw new InvalidOperationException("A serializable Type is required", e);
            }
        }

        /// <summary>
        /// Deserialize array of bytes to T instance
        /// </summary>
        /// <param name="param">Array of bytes</param>
        /// <returns>T representation of the array of bytes</returns>
        protected object Deserialize(byte[] param)
        {
            using (MemoryStream ms = new MemoryStream(param))
            {
                BinaryFormatter br = new BinaryFormatter();
                return br.Deserialize(ms) as object;
            }
        }
    }

    /// <summary>
    /// Inmemory implementation of StateStorage
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class MemoryStateStorage : StateStorage
    {
        object Value = null;
        public override void Clear()
        {
            Value = null;
        }

        public override void Close()
        {
            Clear();
        }

        public override object GetValue()
        {
            return Value;
        }

        public override void Update(object newValue)
        {
            Value = newValue;
        }
    }
}