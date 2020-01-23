using System;
using System.Collections.Generic;
using System.Threading;
using Confluent.Kafka;

namespace Com.RFranco.Streams.Kafka
{
    /// <summary>
    /// A Kakfa sink, that allows to specify message keys.
    /// </summary>
    /// <typeparam name="K">The type of the stream message keys.</typeparam>
    /// <typeparam name="T">The type of the stream messages.</typeparam>
    public class KafkaStreamSink<K, T> : IKeyedStreamSink<K, T>
    {
        private IStreamSource sourceToCommit;
        private readonly ProducerConfig producerConfig;
        private readonly ISerializer<K> keySerializer;
        private readonly ISerializer<T> valueSerializer;
        private readonly string topic;
        private System.Timers.Timer dumpTimer;

        private int NumMessagesInBatch;

        public KafkaStreamSink(ProducerConfig producerConfig, string topic, ISerializer<K> keySerializer, ISerializer<T> valueSerializer)
        {
            this.producerConfig = producerConfig;
            this.topic = topic;
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
            this.CommitTimeout = TimeSpan.FromSeconds(5);
            this.NumMessagesInBatch = 0;
        }

        /// <summary>
        /// When this timeout expires, the sink will wait for the Kafka producer to send
        /// all the messages and will commit to the source. The default is 5 seconds.
        /// </summary>
        public TimeSpan CommitTimeout { get; set; }

        public event Action<StreamingError> OnError;

        public event Action<string> OnStatistics;

        public void Dump(IEnumerable<T> stream, CancellationToken cancellationToken)
        {
            DumpMessages(stream, value => new Message<K, T>{ Value = value }, cancellationToken);
        }

        public void DumpWithKey(IEnumerable<KeyValuePair<K, T>> stream, CancellationToken cancellationToken)
        {
            DumpMessages(stream, keyValuePair => new Message<K, T>{ Key = keyValuePair.Key, Value = keyValuePair.Value }, cancellationToken);
        }

        private void DumpMessages<M>(IEnumerable<M> messages, Func<M, Message<K, T>> getMessage, CancellationToken cancellationToken)
        {
            var producerBuilder = new ProducerBuilder<K, T>(producerConfig);
            producerBuilder.SetKeySerializer(keySerializer);
            producerBuilder.SetValueSerializer(valueSerializer);
            producerBuilder.SetErrorHandler((_, e) => OnError?.Invoke(new StreamingError{ IsFatal = e.IsFatal, Reason = e.Reason }));
            producerBuilder.SetStatisticsHandler((_, statistics) => OnStatistics?.Invoke(statistics));

            using (var p = producerBuilder.Build())
            {
                SetupCommitTimer(() => {
                    lock(p)
                    {
                        if(NumMessagesInBatch > 0) {
                            p.Flush(cancellationToken);
                            NumMessagesInBatch = 0;
                            sourceToCommit?.Commit();
                        }
                    }
                });

                foreach(M message in messages)
                {
                    lock(p)
                    {
                        p.Produce(this.topic, 
                        getMessage.Invoke(message), 
                        r => {
                            if (r.Error.IsError)
                            {
                                OnError?.Invoke(new StreamingError{ IsFatal = r.Error.IsFatal, Reason = r.Error.Reason });
                            } else {
                                NumMessagesInBatch++;
                            }
                        });
                    }                    
                }
            }
        }

        public void SetSourceToCommit(IStreamSource source)
        {
            this.sourceToCommit = source;
        }

        private void SetupCommitTimer(Action onTimeout)
        {
            dumpTimer = new System.Timers.Timer(CommitTimeout.TotalMilliseconds);
            dumpTimer.Elapsed += (sender, eventArgs) => {
                onTimeout.Invoke();
            };
            dumpTimer.AutoReset = true;
            dumpTimer.Enabled = true;            
        }
    }
}