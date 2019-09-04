﻿using System;
using System.Threading;
using Confluent.Kafka;

namespace Com.RFranco.Streams.Kafka
{
    public class KafkaStreamSource<K, T>: IStreamSource<T>
    {
        private readonly ConsumerConfig consumerConfig;
        private readonly string topic;
        private readonly IDeserializer<T> valueDeserializer;
        private IConsumer<Null, T> kafkaConsumer;

        public KafkaStreamSource(ConsumerConfig consumerConfig, string topic, IDeserializer<T> valueDeserializer)
        {
            this.consumerConfig = consumerConfig;
            this.topic = topic;
            this.valueDeserializer = valueDeserializer;
        }

        public event Action OnEOF;
        public event Action<StreamingError> OnError;

        public void Commit()
        {
            kafkaConsumer?.Commit();
        }

        public System.Collections.Generic.IEnumerable<T> Stream(CancellationToken cancellationToken)
        {
            consumerConfig.EnablePartitionEof = true;

            var kafkaConsumerBuilder = new ConsumerBuilder<Null, T>(consumerConfig);
            kafkaConsumerBuilder.SetKeyDeserializer(Deserializers.Null);
            kafkaConsumerBuilder.SetValueDeserializer(valueDeserializer);
            kafkaConsumerBuilder.SetErrorHandler((_, e) => OnError?.Invoke(new StreamingError{ IsFatal = e.IsFatal, Reason = e.Reason }));
            
            using (var kafkaConsumer = kafkaConsumerBuilder.Build())
            {
                this.kafkaConsumer = kafkaConsumer;
                kafkaConsumer.Subscribe(topic);

                while (!cancellationToken.IsCancellationRequested)
                {
                    ConsumeResult<Null, T> consumedResult;
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
                        yield return consumedResult.Value;
                }

                // Gracefully close the consumer, liberating the group offsets, etc.
                kafkaConsumer.Close();
            }

        }
    }
}