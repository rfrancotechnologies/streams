using System;
using System.Collections.Generic;
using Confluent.Kafka;

namespace Com.RFranco.Streams.Kafka.source
{
    /// <summary>
    /// Source of messages which origin is a kafka broker
    /// </summary>
    public interface IKafkaStreamSource<K, T> : IStreamSource<T>
    {
        // <summary>
        /// Set the function to assign the kafka topic partition offsets
        /// </summary>
        void SetPartitionsAssignedHandler(Func<IConsumer<K, T>, List<TopicPartition>, IEnumerable<TopicPartitionOffset>> partitionsAssignedHandle);
}}