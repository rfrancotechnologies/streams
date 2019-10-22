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
    public class ElasticStreamSink<K, T> : IKeyedStreamSink<K, T> where T : class where K : class
    {
        private IElasticClient ElasticClient;
        private IStreamSource sourceToCommit;

        private string Index;

        public event Action<StreamingError> OnError;

        public ElasticStreamSink(IElasticClient elasticClient, string index)
        {
            this.ElasticClient = elasticClient;
            this.Index = index;
        }

        public void Dump(IEnumerable<T> stream, CancellationToken cancellationToken)
        {
            using (cancellationToken.Register(() => cancellationToken.ThrowIfCancellationRequested()))
            {
                foreach (var data in stream.Select(s => new Message<T> { Value = s, Key = Guid.NewGuid().ToString() }))
                    DumpMessage(data);
            }
        }

        public void DumpWithKey(IEnumerable<KeyValuePair<K, T>> stream, CancellationToken cancellationToken)
        {
            using (cancellationToken.Register(() => cancellationToken.ThrowIfCancellationRequested()))
            {
                foreach (var data in stream.Select(s => new Message<T> { Value = s.Value, Key = s.Key.ToString() }))
                    DumpMessage(data);
            }
        }

        private void DumpMessage(Message<T> message)
        {
            var response = ElasticClient.Index(new IndexRequest<T>(message.Value, index: Index,
                    type: typeof(T).Name, id: message.Key));

            if (!response.IsValid) 
            {
                OnError?.Invoke(new StreamingError { Reason = response.OriginalException.Message, IsFatal = false });
                //throw response.OriginalException;
            } else
                sourceToCommit?.Commit();
        }




        public void SetSourceToCommit(IStreamSource source)
        {
            this.sourceToCommit = source;
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