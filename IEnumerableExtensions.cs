using System.Collections.Generic;

namespace Com.RFranco.Streams
{
    public static class IEnumerableExtensions
    {
        public static void Dump<T>(this IEnumerable<T> enumerable, IStreamSink<T> sink)
        {
            sink.Dump(enumerable);
        }
    }
}