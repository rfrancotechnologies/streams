using System;
using System.Runtime.Serialization;

namespace Com.Rfranco.Streams.ChangeTracking.Exceptions
{
    /// <summary>
    /// Exception during the discovery step. Possibles causes:
    ///     - Change tracking is not enabled for the database.
    ///     - The specified table object ID is not valid for the current database.
    ///     - Insufficient permission to the table specified by the object ID.
    /// </summary>
    public class ChangeTrackingDiscoverException : ChangeTrackingException
    {
        public ChangeTrackingDiscoverException()
        {
        }

        public ChangeTrackingDiscoverException(string message) : base(message)
        {
        }

        public ChangeTrackingDiscoverException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected ChangeTrackingDiscoverException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}