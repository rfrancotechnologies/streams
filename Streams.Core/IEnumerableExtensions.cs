using System.Collections.Generic;
using System.Threading;

namespace Com.RFranco.Streams
{
    public static class IEnumerableExtensions
    {
        public static void Dump<T>(this IEnumerable<T> enumerable, IStreamSink<T> sink, CancellationToken cancellationToken)
        {
            sink.Dump(enumerable, cancellationToken);
        }

        public static void Dump<K, T>(this IEnumerable<KeyValuePair<K, T>> enumerable, IKeyedStreamSink<K, T> sink, CancellationToken cancellationToken)
        {
            sink.DumpWithKey(enumerable, cancellationToken);
        }
    }
}