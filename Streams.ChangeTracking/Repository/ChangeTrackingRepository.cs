using System;
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

        IEnumerable<Change> GetOffsetChanges(IDbConnection conn, 
            TrackedTableInformation changeTrackingTableInfo, long changeVersionFrom, long changeVersionTo);
    }

    public class ChangeTrackingRepository : IChangeTrackingRepository
    {
        private readonly ChangeTrackingConfiguration configuration;


        public ChangeTrackingRepository(ChangeTrackingConfiguration configuration)
        {
            this.configuration = configuration;
        }

        public IDbConnection CreateConnection()
        {
            SqlConnection connection = new SqlConnection(configuration.ConnectionString);
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

            foreach (var whiteListTableConfiguration in configuration.WhiteListTables)
            {
                var tableInformation = conn.Query<TrackedTableInformation>(
                  $"SELECT OBJECT_SCHEMA_NAME(object_id) AS SchemaName,"
                + "Object_Name(object_id) AS TableName,"
                + "min_valid_version AS MinValidVersion, "
                + $"'{whiteListTableConfiguration.PrimaryKeyColumn}' AS PrimaryKeyColumn "
                + (configuration.CustomSortEnable ? $",'{whiteListTableConfiguration.SortColumn}' AS SortColumn " : "")
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
        /// Get changes between provied change versions
        /// </summary>
        /// <param name="conn">Database connection</param>
        /// <param name="changeTrackingTableInfo">Tracked table information</param>
        /// <param name="changeVersionFrom">Changes version from</param>
        /// <param name="changeVersionTo">Changes version to</param>
        /// <returns>Detected changes between specified change versions</returns>
        public IEnumerable<Change> GetOffsetChanges(IDbConnection conn, 
            TrackedTableInformation changeTrackingTableInfo, long changeVersionFrom, long changeVersionTo)
        {
            if(configuration.CustomSortEnable && string.IsNullOrEmpty(changeTrackingTableInfo.SortColumn))
                throw new ArgumentException("Sort column of tracked table should be specified if custom sort is enable.");

            var query = configuration.CustomSortEnable ? 
                LoadChangesWithCustomSortColumnQuery(changeTrackingTableInfo, changeVersionFrom, changeVersionTo) :
                LoadChangesSortByPrimaryKeyQuery(changeTrackingTableInfo, changeVersionFrom, changeVersionTo);
            
            return conn.Query<Change>(query).Distinct();
        }
        private string LoadChangesSortByPrimaryKeyQuery(TrackedTableInformation changeTrackingTableInfo, 
            long changeVersionFrom, long changeVersionTo)
        {
            return $"SELECT CT.{changeTrackingTableInfo.PrimaryKeyColumn} AS PrimaryKeyValue, "
                + $"'{changeTrackingTableInfo.GetFullTableName()}' AS TableFullName, "
                + "SYS_CHANGE_OPERATION AS _ChangeOperation, "
                + "SYS_CHANGE_VERSION AS ChangeVersion "
                + $"FROM CHANGETABLE(CHANGES {changeTrackingTableInfo.GetFullTableName()} , {changeVersionFrom}) AS CT "
                + $"WHERE SYS_CHANGE_VERSION <= {changeVersionTo} "
                + $"Order by CT.{changeTrackingTableInfo.PrimaryKeyColumn} DESC";
        }
        private string LoadChangesWithCustomSortColumnQuery(TrackedTableInformation changeTrackingTableInfo, 
            long changeVersionFrom, long changeVersionTo)
        {
            return $"SELECT CT.{changeTrackingTableInfo.PrimaryKeyColumn} AS PrimaryKeyValue, "
                + $"'{changeTrackingTableInfo.GetFullTableName()}' AS TableFullName, "
                + "SYS_CHANGE_OPERATION AS _ChangeOperation, "
                + "SYS_CHANGE_VERSION AS ChangeVersion, "
                + $"T.{changeTrackingTableInfo.SortColumn} AS SortColumn "
                + $"FROM  CHANGETABLE(CHANGES {changeTrackingTableInfo.GetFullTableName()} , {changeVersionFrom}) AS CT "
                + $"INNER JOIN {changeTrackingTableInfo.GetFullTableName()} T "
                + $"    ON T.{changeTrackingTableInfo.PrimaryKeyColumn} = CT.{changeTrackingTableInfo.PrimaryKeyColumn} "
                + $"WHERE SYS_CHANGE_VERSION <= {changeVersionTo} ";
        }
    }
}