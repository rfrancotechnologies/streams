using System;
using System.Runtime.Serialization;

namespace Com.Rfranco.Streams.ChangeTracking.Exceptions
{
    /// <summary>
    /// Exception thrown when change tracking is not enabled for the database
    /// </summary>
    public class ChangeTrackingException : Exception
    {
        public ChangeTrackingException()
        {
        }

        public ChangeTrackingException(string message) : base(message)
        {
        }

        public ChangeTrackingException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected ChangeTrackingException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}