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
        /// </summary>
        /// <param name="stream">Stream that will be dumped into the sink.</param>
        /// <param name="cancellationToken">Token that indicates that the execution must be stopped.</param>
        void Dump(IEnumerable<T> stream, CancellationToken cancellationToken);

        /// <summary>
        /// Indicate that this sink must explicitly commit to the given source when a batch of messages has been successfully dumped.
        /// </summary>
        /// <param name="source">The source of messages that must be commited when a batch of messages has been successfully dumped.</param>
        void SetSourceToCommit(IStreamSource source);

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
    }
}