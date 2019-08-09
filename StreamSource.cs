using System;
using System.Collections.Generic;

namespace Streams
{
    /// <summary>
    /// Source of messages. Sources are able to produce a bounded or unbounded IEnumerable of messages.
    /// </summary>
    /// <typeparam name="T">The type of the messages that this source produces.</typeparam>
    public interface IStreamSource<T>
    {
        /// <summary>
        /// Retrieves the stream of messages from the source.
        /// </summary>
        /// <returns>IEnuerable of messages of type T.</returns>
        IEnumerable<T> Stream();

        /// <summary>
        /// Event invoked each time the source reaches the end. For instance, each time a Kafka sources reaches
        /// the end of a partition or each time a SQL Server change-tracker detects that no new changes have been added to the database.
        /// </summary>
        event Action OnEOF;

        /// <summary>
        /// Event invoked each time an error occurs in the source.
        /// </summary>
        event Action<StreamingError> OnError;
    }
}
