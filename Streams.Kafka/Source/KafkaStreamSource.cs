using System;
using System.Collections.Generic;
using System.Threading;
using Confluent.Kafka;

namespace Com.RFranco.Streams.Kafka.source
{
    public class KafkaStreamSource<K, T>: IKafkaStreamSource<K, T>
    {        
        private readonly ConsumerConfig consumerConfig;
        private readonly string topic;
        private readonly IDeserializer<T> valueDeserializer;
        private readonly IDeserializer<K> keyDeserializer;
        private IConsumer<K, T> kafkaConsumer;
        private Func<IConsumer<K, T>, List<TopicPartition>, IEnumerable<TopicPartitionOffset>> partitionsAssignedHandle;

        public KafkaStreamSource(ConsumerConfig consumerConfig, string topic, IDeserializer<K> keyDeserializer, IDeserializer<T> valueDeserializer)
        {
            this.consumerConfig = consumerConfig;
            consumerConfig.EnablePartitionEof = true;

            this.topic = topic;
            this.keyDeserializer = keyDeserializer;
            this.valueDeserializer = valueDeserializer;
        }

        public event Action OnEOF;
        public event Action<StreamingError> OnError;
        public event Action<string> OnStatistics;
        public bool CommitEnable { get; set; } = true;

        public void Commit()
        {
            if(CommitEnable)
                kafkaConsumer?.Commit();
        }

        public IEnumerable<T> Stream(CancellationToken cancellationToken)
        {
            var kafkaConsumerBuilder = CreateKafkaConsumerBuilder();
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
        private ConsumerBuilder<K, T> CreateKafkaConsumerBuilder()
        {
            var kafkaConsumerBuilder = new ConsumerBuilder<K, T>(consumerConfig);
            kafkaConsumerBuilder.SetKeyDeserializer(keyDeserializer);
            kafkaConsumerBuilder.SetValueDeserializer(valueDeserializer);
            kafkaConsumerBuilder.SetErrorHandler((_, e) => OnError?.Invoke(new StreamingError{ IsFatal = e.IsFatal, Reason = e.Reason }));
            kafkaConsumerBuilder.SetStatisticsHandler((_, statistics) => OnStatistics?.Invoke(statistics));

            if(partitionsAssignedHandle != null && CommitEnable)
            {
                throw new ArgumentException("The partition assigned handle can not been setted if " +
                    "'CommitEnable' property was setted to true.");
            }
            else if(partitionsAssignedHandle != null)
            {
                kafkaConsumerBuilder.SetPartitionsAssignedHandler(partitionsAssignedHandle);
            }

            return kafkaConsumerBuilder;
        }

        public void SetPartitionsAssignedHandler(Func<IConsumer<K, T>, List<TopicPartition>, IEnumerable<TopicPartitionOffset>> partitionsAssignedHandle)
        {
            this.partitionsAssignedHandle = partitionsAssignedHandle;
        }
    }
}