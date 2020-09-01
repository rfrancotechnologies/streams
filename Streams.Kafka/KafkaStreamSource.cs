using System;
using System.Threading;
using Confluent.Kafka;

namespace Com.RFranco.Streams.Kafka
{
    public class KafkaStreamSource<K, T>: IStreamSource<T>
    {
        private readonly ConsumerConfig consumerConfig;
        private readonly string topic;
        private readonly IDeserializer<T> valueDeserializer;
        private readonly IDeserializer<K> keyDeserializer;
        private IConsumer<K, T> kafkaConsumer;

        public KafkaStreamSource(ConsumerConfig consumerConfig, string topic, IDeserializer<K> keyDeserializer, IDeserializer<T> valueDeserializer)
        {
            this.consumerConfig = consumerConfig;
            this.topic = topic;
            this.keyDeserializer = keyDeserializer;
            this.valueDeserializer = valueDeserializer;
        }

        public event Action OnEOF;
        public event Action<StreamingError> OnError;
        public event Action<string> OnStatistics;

        public void Commit()
        {
            kafkaConsumer?.Commit();
        }

        public System.Collections.Generic.IEnumerable<T> Stream(CancellationToken cancellationToken)
        {
            consumerConfig.EnablePartitionEof = true;

            var kafkaConsumerBuilder = new ConsumerBuilder<K, T>(consumerConfig);
            kafkaConsumerBuilder.SetKeyDeserializer(keyDeserializer);
            kafkaConsumerBuilder.SetValueDeserializer(valueDeserializer);
            kafkaConsumerBuilder.SetErrorHandler((_, e) => OnError?.Invoke(new StreamingError{ IsFatal = e.IsFatal, Reason = e.Reason }));
            kafkaConsumerBuilder.SetStatisticsHandler((_, statistics) => OnStatistics?.Invoke(statistics));
            
            using (var kafkaConsumer = kafkaConsumerBuilder.Build())
            {
                this.kafkaConsumer = kafkaConsumer;
                kafkaConsumer.Subscribe(topic);

                while (!cancellationToken.IsCancellationRequested)
                {
                    ConsumeResult<K, T> consumedResult;
                    try
                    {
                        consumedResult = kafkaConsumer.Consume(cancellationToken);

                        if (consumedResult?.IsPartitionEOF == true)
                        {
                            OnEOF?.Invoke();                            
                        }
                    }
                    catch (ConsumeException ex)
                    {
                        OnError?.Invoke(new StreamingError { Reason = ex.Message, IsFatal = false });
                        consumedResult = null;
                    }
                    catch (OperationCanceledException) {
                        consumedResult = null;
                    }

                    if (consumedResult?.Message != null)
                        yield return consumedResult.Message.Value;
                }

                // Gracefully close the consumer, liberating the group offsets, etc.
                kafkaConsumer.Close();
            }

        }
    }
}
