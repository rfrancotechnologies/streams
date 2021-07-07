using Com.Rfranco.Streams.ChangeTracking.Models;
using Com.RFranco.Streams;

namespace Com.Rfranco.Streams.ChangeTracking.Source
{
    /// <summary>
    /// Source of messages which origin is a SQLServer table
    /// </summary>
    public interface IChangeTrackingStreamSource : IStreamSource<Change>
    {
        // <summary>
        /// Initialize the SQL Server CHANGETABLE where by the source should start
        /// </summary>
        void SetInitialChangeTable(long initialChangeTable);
    }
}