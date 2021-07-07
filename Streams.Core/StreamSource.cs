using System;
using System.Collections.Generic;
using System.Threading;

namespace Com.RFranco.Streams
{
    /// <summary>
    /// Source of messages. 
    /// </summary>
    public interface IStreamSource
    {
        /// <summary>
        /// Event invoked each time the source reaches the end. For instance, each time a Kafka sources reaches
        /// the end of a partition or each time a SQL Server change-tracker detects that no new changes have been added to the database.
        /// </summary>
        event Action OnEOF;

        /// <summary>
        /// Event invoked each time an error occurs in the source.
        /// </summary>
        event Action<StreamingError> OnError;

        /// <summary>
        /// Mark all the messages consumed up to this point as successfully processed. These messages
        /// must not be consumed again in subsequent streaming processings.
        /// </summary>
        void Commit();
    }

    /// <summary>
    /// Source of messages of type T. Sources are able to produce a bounded or unbounded IEnumerable of messages of type T.
    /// </summary>
    /// <typeparam name="T">The type of the messages that this source produces.</typeparam>
    public interface IStreamSource<T>: IStreamSource
    {
        /// <summary>
        /// Enable/Disable the stream source commit of the published messages.
        /// </summary>
        bool CommitEnable {get; set;}
        
        /// <summary>
        /// Retrieves the stream of messages from the source.
        /// </summary>
        /// <returns>IEnuerable of messages of type T.</returns>
        IEnumerable<T> Stream(CancellationToken cancellationToken);
    }
}
