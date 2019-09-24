using System.Collections.Generic;
using System.Data;
using System.Linq;
using Com.Rfranco.Streams.ChangeTracking.Config;
using Com.Rfranco.Streams.ChangeTracking.Exceptions;
using Com.Rfranco.Streams.ChangeTracking.Models;
using Com.RFranco.Streams.State;
using Dapper;

namespace Com.Rfranco.Streams.ChangeTracking
{
    /// <summary>
    /// Interface with operations to discover and manage the changes on the tracked tables configured
    /// </summary>
    public interface IChangeTrackingManager
    {
        /// <summary>
        /// Retrieve the list of changes detected (or initial status)
        /// </summary>
        /// <param name="conn">Database connection</param>
        /// <param name="applicationOffset">Current offset state of the application</param>
        /// <exception cref="UnsupportedMinimalVersionException">Thrown when the application status (last_synchronization_version) is older than the minimum valid synchronization version for a table</exception>
        IEnumerable<Change> GetChanges(IDbConnection conn, long? applicationOffset);

        /// <summary>
        /// Retrieve the current database offset
        /// </summary>
        /// <param name="conn">Database connection</param>
        /// <returns>Current Database Offset value</returns>
        /// <exception cref="ChangeTrackingDisabledException">Thrown when change tracking is not enabled for the database.</exception>
        long GetDatabaseOffset(IDbConnection conn);

    }

    /// <summary>
    /// IChangeTrackingManager implementation
    /// </summary>
    class ChangeTrackingManager : IChangeTrackingManager
    {
        private ChangeTrackingConfiguration Configuration;

        public ChangeTrackingManager(ChangeTrackingConfiguration configuration)
        {
            this.Configuration = configuration;
        }

        /// <summary>
        /// Read and process all the table changes from the provided tables.
        /// 
        /// Table changes are first read from lower priority to higher priority tables, and subsequently processed
        /// from higher priority tables to lower priority ones. This is made in order to prevent processing changes 
        /// from dependent entities that haven't yet been processed (keeping consistency with dependent entities and
        /// the generated events). 
        /// For instance, assuming we are processing player wallets and player wallet transactions (wallets have higher
        /// priority than transactions), to prevent reading player wallet transactions from wallets that have not yet been
        /// read we first read changes in player wallet transactions, then we read changes in wallets, then we process the
        /// changes in wallets and finally we process the change in transactions.
        /// </summary>
        /// <param name="conn">The database connection.</param>
        /// <param name="applicationOffset">The current offset state of the application (global to the whole application, not to specific tables).</param>
        public IEnumerable<Change> GetChanges(IDbConnection conn, long? applicationOffset)
        {
            Stack<IEnumerable<Change>> tableChanges = new Stack<IEnumerable<Change>>();
            IEnumerable<TrackedTableInformation> changeTrackingTableInfos = GetTrackedTablesInformation(conn);
            foreach (var changeTrackingTableInfo in changeTrackingTableInfos.OrderBy(x => x.Priority))
            {
                if (!applicationOffset.HasValue)
                {
                    yield return new Change{TableFullName = changeTrackingTableInfo.GetFullTableName()};  
                }
                else
                {
                    if (applicationOffset != 0 && applicationOffset < changeTrackingTableInfo.MinValidVersion)
                    {
                        throw new UnsupportedMinimalVersionException($"Received a minimal offset supported {changeTrackingTableInfo.MinValidVersion}"
                        + $" higher than the last processed {applicationOffset} for {changeTrackingTableInfo.GetFullTableName()} table");
                    }
                    else
                    {
                        tableChanges.Push(GetOffsetChanges(conn, changeTrackingTableInfo, applicationOffset.Value));
                    }
                }
            }

            while (tableChanges.Any())
            {
                foreach (var change in tableChanges.Pop())
                {
                    yield return change;
                }
            }
        }

        /// <summary>
        /// Get the current database offset
        /// </summary>
        /// <param name="conn">Database connection</param>
        /// <returns>Current database offset</returns>
        /// <exception cref="ChangeTrackingDisabledException">Thrown when change tracking is not enabled for the database.</exception>
        public long GetDatabaseOffset(IDbConnection conn)
        {
            var databaseOffset = conn.Query<long?>("SELECT CHANGE_TRACKING_CURRENT_VERSION() AS CurrentOffset").SingleOrDefault();
            if (!databaseOffset.HasValue) throw new ChangeTrackingDisabledException("Change tracking is not enabled for the database configured");

            return databaseOffset.Value;
        }

        /// <summary>
        /// Retrieve the tracking information of the tracked tables configured
        /// </summary>
        /// <param name="conn">Database connection</param>
        /// <returns>List with the tracking information of each table configured</returns>        
        private IEnumerable<TrackedTableInformation> GetTrackedTablesInformation(IDbConnection conn)
        {
            List<TrackedTableInformation> tableInformations = new List<TrackedTableInformation>();

            foreach (var whiteListTableConfiguration in Configuration.WhiteListTables)
            {
                var tableInformation = conn.Query<TrackedTableInformation>(
                  $"SELECT OBJECT_SCHEMA_NAME(object_id) AS SchemaName, Object_Name(object_id) AS TableName,"
                + $" object_id AS ObjectId, is_track_columns_updated_on AS Enabled, min_valid_version AS MinValidVersion,"
                + $" begin_version AS BeginVersion, cleanup_version AS CleanUpVersion, '{whiteListTableConfiguration.PrimaryKeyColumn}' AS PrimaryKeyColumn"
                + $" FROM sys.change_tracking_tables WHERE CONCAT(OBJECT_SCHEMA_NAME(object_id),'.', Object_Name(object_id)) = @TableName",
                    new
                    {
                        TableName = whiteListTableConfiguration.FullName
                    }).SingleOrDefault();

                if (null == tableInformation) throw new ChangeTrackingDiscoverException("No change tracking detected on table " + whiteListTableConfiguration.FullName);

                tableInformation.Priority = whiteListTableConfiguration.Priority;
                tableInformations.Add(tableInformation);
            }

            return tableInformations;
        }

        /// <summary>
        /// Gete changes since offset
        /// </summary>
        /// <param name="conn">Database connection</param>
        /// <param name="changeTrackingTableInfo">Tracked table information</param>
        /// <param name="offset">Current offset</param>
        /// <returns>Changes since offset provided</returns>
        private IEnumerable<Change> GetOffsetChanges(IDbConnection conn, TrackedTableInformation changeTrackingTableInfo, long offset)
        {
            return conn.Query<Change>(
                $"SELECT CT.{changeTrackingTableInfo.PrimaryKeyColumn} AS PrimaryKeyValue,"
                + $"SYS_CHANGE_VERSION AS ChangeVersion,"
                + $"SYS_CHANGE_CREATION_VERSION AS ChangeCreationVersion,"
                + $"SYS_CHANGE_OPERATION AS _ChangeOperation,"
                + $"SYS_CHANGE_COLUMNS AS ChangeColumns,"
                + $"SYS_CHANGE_CONTEXT AS ChangeContext, '{changeTrackingTableInfo.GetFullTableName()}' AS TableFullName"
                + $" FROM  CHANGETABLE(CHANGES {changeTrackingTableInfo.GetFullTableName()} , {offset}) AS CT Order by CT.{changeTrackingTableInfo.PrimaryKeyColumn} ASC");
        }
    }
}