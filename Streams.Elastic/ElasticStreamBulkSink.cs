using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Nest;

namespace Com.RFranco.Streams.Elastic
{
    /// <summary>
    /// A Kakfa sink, that allows to specify message keys.
    /// </summary>
    /// <typeparam name="K">The type of the stream message keys.</typeparam>
    /// <typeparam name="T">The type of the stream messages.</typeparam>
    public class ElasticStreamBulkSink<K, T> : IKeyedStreamSink<K, T> where T : class where K : class
    {
        private IElasticClient ElasticClient;
        private IStreamSource sourceToCommit;

        private string Index;

        private System.Timers.Timer dumpTimer;

        private List<Message<T>> DataBuffer;


        /// <summary>
        /// When this timeout expires, the sink will wait for the Kafka producer to send
        /// all the messages and will commit to the source. The default is 5 seconds.
        /// </summary>
        public TimeSpan BulkTimeout { get; set; }

        public event Action<StreamingError> OnError;

        private long MaxBufferSize;

        public ElasticStreamBulkSink(IElasticClient elasticClient, string index, TimeSpan bulkTimeoutSeconds, long maxBufferSize = 1000)
        {
            this.ElasticClient = elasticClient;
            this.Index = index;
            this.BulkTimeout = bulkTimeoutSeconds;
            this.DataBuffer = new List<Message<T>>();
            this.MaxBufferSize = maxBufferSize;
            SetupCommitTimer(() =>
                {
                    lock (DataBuffer)
                    {
                        if (DataBuffer.Count() != 0)
                            DumpMessages();
                    }
                });
        }

        public void Dump(IEnumerable<T> stream, CancellationToken cancellationToken)
        {
            using (cancellationToken.Register(() => cancellationToken.ThrowIfCancellationRequested()))
            {
                foreach (var data in stream.Select(s => new Message<T> { Value = s, Key = Guid.NewGuid().ToString() }))
                {
                    lock (DataBuffer)
                    {
                        DataBuffer.Add(data);
                        if(DataBuffer.Count() >= MaxBufferSize)
                            DumpMessages();
                    }
                }
            }
        }

        public void DumpWithKey(IEnumerable<KeyValuePair<K, T>> stream, CancellationToken cancellationToken)
        {
            using (cancellationToken.Register(() => cancellationToken.ThrowIfCancellationRequested()))
            {
                foreach (var data in stream.Select(s => new Message<T> { Value = s.Value, Key = s.Key.ToString() }))
                {
                    lock (DataBuffer)
                    {
                        DataBuffer.Add(data);
                        if(DataBuffer.Count() >= MaxBufferSize)
                            DumpMessages();
                    }
                }
            }
        }

        private void DumpMessages()
        {
            var response = ElasticClient.Bulk(b => b.Index(Index)
            .IndexMany(DataBuffer, (descriptor, s) => descriptor.Index(Index).Id(s.Key)));

            if (!response.IsValid)
            {
                OnError?.Invoke(new StreamingError { Reason = response.OriginalException.Message, IsFatal = false });
                throw response.OriginalException;
            }
            else
            {
                sourceToCommit?.Commit();
                DataBuffer.Clear();
            }
        }


        public void SetSourceToCommit(IStreamSource source)
        {
            this.sourceToCommit = source;
        }


        private void SetupCommitTimer(Action onTimeout)
        {
            dumpTimer = new System.Timers.Timer(BulkTimeout.TotalMilliseconds);
            dumpTimer.Elapsed += (sender, eventArgs) =>
            {
                onTimeout.Invoke();
            };
            dumpTimer.AutoReset = true;
            dumpTimer.Enabled = true;
        }

        //
        // Resumen:
        //     Represents a (deserialized) Kafka message.
        private class Message<TValue>
        {
            //
            // Resumen:
            //     Gets the message key value (possibly null).
            public string Key { get; set; }
            //
            // Resumen:
            //     Gets the message value (possibly null).
            public TValue Value { get; set; }
        }
    }
}