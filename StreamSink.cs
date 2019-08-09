using System;
using System.Collections.Generic;

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
        void Dump(IEnumerable<T> stream);

        /// <summary>
        /// Indicate that this sink must explicitly commit to the given source when a batch of messages has been successfully dumped.
        /// </summary>
        /// <param name="source">The source of messages that must be commited when a batch of messages has been successfully dumped.</param>
        /// <typeparam name="S">The type of source messagges.</typeparam>
        void SetSourceToCommit<S>(IStreamSource<S> source);

        /// <summary>
        /// Event invoked when an error occurs in the sink.
        /// </summary>
        event Action<StreamingError> OnError;

        /// <summary>
        /// The maximum size of the messages batch. When this number of messages is reached, the batch will be effectively dumped into the sink.
        /// </summary>
        int MaxBatchSize { get; set; }

        /// <summary>
        /// Timeout for dumping a batch of messages into the sink. If the timeout is reached, even if the number of messages
        /// has not reached MaxBatchSize, the batch will be effectively dumped into the sink.
        /// </summary>
        TimeSpan BatchTimeout { get; set; }
    }
}