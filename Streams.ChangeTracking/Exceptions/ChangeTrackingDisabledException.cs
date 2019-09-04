using System;
using System.Runtime.Serialization;

namespace Com.Rfranco.Streams.ChangeTracking.Exceptions
{
    /// <summary>
    /// Exception thrown when change tracking is not enabled for the database
    /// </summary>
    public class ChangeTrackingDisabledException : ChangeTrackingException
    {
        public ChangeTrackingDisabledException()
        {
        }

        public ChangeTrackingDisabledException(string message) : base(message)
        {
        }

        public ChangeTrackingDisabledException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected ChangeTrackingDisabledException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}