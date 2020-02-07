using System;
using System.Collections.Generic;
using System.Threading;

namespace Com.RFranco.Streams
{
    /// <summary>
    /// A sink of messages. Sinks receive streams of messages and dump them into some output (like ElasticSearch, some Kafka topic or a CSV file).
    /// </summary>
    /// <typeparam name="T">The type of the messages that the sink is able to process.</typeparam>
    public interface IStreamSink<T>
    {
        /// <summary>
        /// Dump the stream into the sink.
        /// Use void Dump(IEnumerable<T> stream, Func<T, DateTimeOffset> getMessageDateTime, CancellationToken cancellationToken) 
        /// to override the date time of the message.
        /// </summary>
        /// <param name="stream">Stream that will be dumped into the sink.</param>
        /// <param name="cancellationToken">Token that indicates that the execution must be stopped.</param>
        
        void Dump(IEnumerable<T> stream, CancellationToken cancellationToken);

        /// <summary>
        /// Dump the stream into the sink.
        /// </summary>
        /// <param name="stream">Stream that will be dumped into the sink.</param>
        /// <param name="getMessageDateTime">Function to indicate how to extract the date time of the message to be dumped.
        /// <param name="cancellationToken">Token that indicates that the execution must be stopped.</param>
        void Dump(IEnumerable<T> stream, Func<T, DateTimeOffset> getMessageDateTime, CancellationToken cancellationToken);

        /// <summary>
        /// An action to invoke when the sink has been successfully dumped the messages to this point.
        /// </summary>        
        event Action OnCommit;

        /// <summary>
        /// Event invoked when an error occurs in the sink.
        /// </summary>
        event Action<StreamingError> OnError;
    }

    /// <summary>
    /// Sinks that accept messages with keys.
    /// </summary>
    /// <typeparam name="K">The type of the message keys.</typeparam>
    /// <typeparam name="T">The type of the messages that the sink is able to process.</typeparam>
    public interface IKeyedStreamSink<K, T>: IStreamSink<T>
    {
        /// <summary>
        /// Dump the stream into the sink using the key, from the provided KeyValuePair, as message key for the sink.
        /// For instance, a Kafka message with a key or a ElasticSearch document with a specific UID.        
        /// </summary>
        /// <param name="stream">Stream that will be dumped into the sink.</param>
        /// <param name="cancellationToken">Token that indicates that the execution must be stopped.</param>
        void DumpWithKey(IEnumerable<KeyValuePair<K,T>> stream, CancellationToken cancellationToken);

        /// <summary>
        /// Dump the stream into the sink using the key, from the provided KeyValuePair, as message key for the sink.
        /// For instance, a Kafka message with a key or a ElasticSearch document with a specific UID.
        /// </summary>
        /// <param name="stream">Stream that will be dumped into the sink.</param>
        /// <param name="getMessageDateTime">Function to indicate how to extract the date time of the message to be dumped.
        /// <param name="cancellationToken">Token that indicates that the execution must be stopped.</param>
        void DumpWithKey(IEnumerable<KeyValuePair<K,T>> stream, Func<T, DateTimeOffset> getMessageDateTime, CancellationToken cancellationToken);
    }
}