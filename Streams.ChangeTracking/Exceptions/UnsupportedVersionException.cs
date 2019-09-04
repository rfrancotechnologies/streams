using System;
using System.Runtime.Serialization;

namespace Com.Rfranco.Streams.ChangeTracking.Exceptions
{
    /// <summary>
    /// Exception during the Resolver step.
    /// This kind of exception represents a last_synchronization_version older than minimal version supported for the table situation.
    /// Information about changes is maintained for a limited time. 
    /// If the value of last_synchronization_version is not valid, the application must reinitialize all the data.
    /// </summary>
    public class UnsupportedMinimalVersionException : ChangeTrackingException
    {
        public UnsupportedMinimalVersionException()
        {
        }

        public UnsupportedMinimalVersionException(string message) : base(message)
        {
        }

        public UnsupportedMinimalVersionException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected UnsupportedMinimalVersionException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}