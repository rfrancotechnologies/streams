using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using Com.Rfranco.Streams.ChangeTracking.Config;
using Com.Rfranco.Streams.ChangeTracking.Exceptions;
using Com.Rfranco.Streams.ChangeTracking.Models;
using Dapper;

namespace Com.Rfranco.Streams.ChangeTracking.Repositories
{
    public interface IChangeTrackingRepository
    {
        /// <summary>
        /// Create conection
        /// </summary>
        /// <returns></returns>
        IDbConnection CreateConnection();
        
        long GetDatabaseOffset(IDbConnection conn);

        IEnumerable<TrackedTableInformation> GetTrackedTablesInformation(IDbConnection conn);

        IEnumerable<Change> GetOffsetChanges(IDbConnection conn, TrackedTableInformation changeTrackingTableInfo, 
            long changeVersionFrom, long changeVersionTo);
    }

    public class ChangeTrackingRepository : IChangeTrackingRepository
    {
        private ChangeTrackingConfiguration Configuration;


        public ChangeTrackingRepository(ChangeTrackingConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IDbConnection CreateConnection()
        {
            SqlConnection connection = new SqlConnection(Configuration.ConnectionString);
            if (connection.State != ConnectionState.Open)
                connection.Open();
            return connection;
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
            if (!databaseOffset.HasValue) throw new ChangeTrackingException("Change tracking is not enabled for the database configured");

            return databaseOffset.Value;
        }

        /// <summary>
        /// Retrieve the tracking information of the tracked tables configured
        /// </summary>
        /// <param name="conn">Database connection</param>
        /// <returns>List with the tracking information of each table configured</returns>        
        public IEnumerable<TrackedTableInformation> GetTrackedTablesInformation(IDbConnection conn)
        {
            List<TrackedTableInformation> tableInformations = new List<TrackedTableInformation>();

            foreach (var whiteListTableConfiguration in Configuration.WhiteListTables)
            {
                var tableInformation = conn.Query<TrackedTableInformation>(
                  $"SELECT OBJECT_SCHEMA_NAME(object_id) AS SchemaName,"
                + "Object_Name(object_id) AS TableName,"
                + "min_valid_version AS MinValidVersion,"
                + $"'{whiteListTableConfiguration.PrimaryKeyColumn}' AS PrimaryKeyColumn "
                + $" FROM sys.change_tracking_tables WHERE CONCAT(OBJECT_SCHEMA_NAME(object_id),'.', Object_Name(object_id)) = @TableName",
                    new
                    {
                        TableName = whiteListTableConfiguration.FullName
                    }).SingleOrDefault();

                if (null == tableInformation) throw new ChangeTrackingException("No change tracking detected on table " + whiteListTableConfiguration.FullName);

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
        public IEnumerable<Change> GetOffsetChanges(IDbConnection conn, TrackedTableInformation changeTrackingTableInfo, 
            long changeVersionFrom, long changeVersionTo)
        {
            return conn.Query<Change>(
                $"SELECT CT.{changeTrackingTableInfo.PrimaryKeyColumn} AS PrimaryKeyValue, "
                + $"'{changeTrackingTableInfo.GetFullTableName()}' AS TableFullName, "
                + "SYS_CHANGE_OPERATION AS _ChangeOperation, "
                + "SYS_CHANGE_VERSION AS ChangeVersion "
                + $"FROM  CHANGETABLE(CHANGES {changeTrackingTableInfo.GetFullTableName()} , {changeVersionFrom}) AS CT "
                + $"WHERE SYS_CHANGE_VERSION <= {changeVersionTo} "
                + $"Order by CT.{changeTrackingTableInfo.PrimaryKeyColumn} ASC")
                .Distinct();
        }
    }
}