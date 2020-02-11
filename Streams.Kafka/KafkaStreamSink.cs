using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Confluent.Kafka;
using Polly;

namespace Com.RFranco.Streams.Kafka
{
    /// <summary>
    /// A Kakfa sink, that allows to specify message keys.
    /// </summary>
    /// <typeparam name="K">The type of the stream message keys.</typeparam>
    /// <typeparam name="T">The type of the stream messages.</typeparam>
    public class KafkaStreamSink<K, T> : IKeyedStreamSink<K, T>
    {
        private readonly ProducerConfig producerConfig;
        private readonly ISerializer<K> keySerializer;
        private readonly ISerializer<T> valueSerializer;
        private readonly string topic;
        private int CommitTimeoutMs;

        public KafkaStreamSink(ProducerConfig producerConfig, string topic, ISerializer<K> keySerializer, ISerializer<T> valueSerializer, int commitTimeoutMs = 5000)
        {
            this.producerConfig = producerConfig;
            this.topic = topic;
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
            this.CommitTimeoutMs = commitTimeoutMs;
        }

        public event Action<StreamingError> OnError;

        public event Action<string> OnStatistics;
        public event Action OnCommit;

        public void Dump(IEnumerable<T> stream, CancellationToken cancellationToken)
        {
            DumpMessages(stream, value => new Message<K, T> { Value = value }, cancellationToken);
        }

        public void Dump(IEnumerable<T> stream, Func<T, DateTimeOffset> getMessageDateTime, CancellationToken cancellationToken)
        {
            DumpMessages(stream, value => new Message<K, T>
            {
                Value = value,
                Timestamp = new Timestamp(getMessageDateTime(value))
            }, cancellationToken);
        }

        public void DumpWithKey(IEnumerable<KeyValuePair<K, T>> stream, CancellationToken cancellationToken)
        {
            DumpMessages(stream, keyValuePair => new Message<K, T> { Key = keyValuePair.Key, Value = keyValuePair.Value }, cancellationToken);
        }

        public void DumpWithKey(IEnumerable<KeyValuePair<K, T>> stream, Func<T, DateTimeOffset> getMessageDateTime, CancellationToken cancellationToken)
        {
            DumpMessages(stream, keyValuePair => new Message<K, T>
            {
                Key = keyValuePair.Key,
                Value = keyValuePair.Value,
                Timestamp = new Timestamp(getMessageDateTime(keyValuePair.Value))
            }, cancellationToken);
        }

        private void DumpMessages<M>(IEnumerable<M> messages, Func<M, Message<K, T>> getMessage, CancellationToken cancellationToken)
        {
            var producerBuilder = new ProducerBuilder<K, T>(producerConfig);
            producerBuilder.SetKeySerializer(keySerializer);
            producerBuilder.SetValueSerializer(valueSerializer);
            producerBuilder.SetErrorHandler((_, e) => OnError?.Invoke(new StreamingError { IsFatal = e.IsFatal, Reason = e.Reason }));
            producerBuilder.SetStatisticsHandler((_, statistics) => OnStatistics?.Invoke(statistics));

            Stopwatch processTime = Stopwatch.StartNew();

            using (var p = producerBuilder.Build())
            {
                foreach (M message in messages)
                {
                    if (cancellationToken.IsCancellationRequested)
                        break;

                    Policy.Handle<ProduceException<K, T>>()
                    .WaitAndRetryForever(retryAttempt => TimeSpan.FromMilliseconds(Math.Min(100 * Math.Pow(2, retryAttempt), 10000)),
                    (exception, timespan) =>
                    {
                        var kafkaException = exception as ProduceException<K, T>;
                        OnError?.Invoke(new StreamingError
                        {
                            IsFatal = kafkaException.Error.IsFatal,
                            Reason = $"{kafkaException.Error.Reason}. The message with key {kafkaException.DeliveryResult.Key} in topic {kafkaException.DeliveryResult.Topic} will be resent on {timespan.TotalMilliseconds} ms."
                        });
                    })
                    .Execute(() =>
                    {
                        if (!cancellationToken.IsCancellationRequested)
                        {
                            p.Produce(this.topic, getMessage.Invoke(message),
                            r =>
                            {
                                if (r.Error.IsError)
                                    OnError?.Invoke(new StreamingError { IsFatal = r.Error.IsFatal, Reason = r.Error.Reason });
                            });
                        }
                    });

                    if (processTime.ElapsedMilliseconds >= CommitTimeoutMs)
                    {
                        p.Flush(cancellationToken);
                        if (!cancellationToken.IsCancellationRequested)
                        {
                            OnCommit?.Invoke();
                            processTime.Restart();
                        }
                    }
                }

                p.Flush(cancellationToken);

                if (!cancellationToken.IsCancellationRequested)
                    OnCommit?.Invoke();
            }
        }
    }
}