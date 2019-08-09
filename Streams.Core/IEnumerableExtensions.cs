using System.Collections.Generic;

namespace Com.RFranco.Streams
{
    public static class IEnumerableExtensions
    {
        public static void Dump<T>(this IEnumerable<T> enumerable, IStreamSink<T> sink)
        {
            sink.Dump(enumerable);
        }

        public static void Dump<K, T>(this IEnumerable<KeyValuePair<K, T>> enumerable, IKeyedStreamSink<K, T> sink)
        {
            sink.DumpWithKey(enumerable);
        }
    }
}