using System.Collections.Generic;

namespace Com.Rfranco.Streams.ChangeTracking.Config
{
    /// <summary>
    /// Configuration of the ChangeTrackingStreamSource instance
    /// </summary>
    public class ChangeTrackingConfiguration
    {
        /// <summary>
        /// Database connection. 
        /// </summary>
        /// <value>String with the database connection</value>
        public string ConnectionString { get; set; }

        /// <summary>
        /// Name of the current application / engine
        /// </summary>
        /// <value>String with the application name</value>
        public string ApplicationName { get; set; }

        /// <summary>
        /// Define the time elapsed between two executions of the change tracking detection loop
        /// </summary>
        /// <value>Long that represents the minimum time that has to be elapsed in milliseconds</value>
        public long PollIntervalMilliseconds { get; set; }

        /// <summary>
        /// Enable/Disable a custom sort based on specific column of each table
        /// </summary>
        /// <value>bool to enable or disable the custom sort</value>
        public bool CustomSortEnable { get; set; } = false;

        /// <summary>
        /// IEnumerable with information related to the tables configured with change tracking
        /// </summary>
        /// <value>ChangeTrackingTableConfiguration instance white table configured</value>
        public IEnumerable<ChangeTrackingTableConfiguration> WhiteListTables { get; set; }
    }

    /// <summary>
    /// Information related to the table configured with change tracking
    /// </summary>
    public class ChangeTrackingTableConfiguration
    {
        /// <summary>
        /// Full name of the table
        /// </summary>
        /// <value>string with the full name of the table. Format: Schema.TableName </value>
        public string FullName { get; set; }

        /// <summary>
        /// Primary key column name of the table
        /// </summary>
        /// <value>string with the primary key column name of the table</value>
        public string PrimaryKeyColumn { get; set; }

        /// <summary>
        /// Priority to process the changes of this table
        /// </summary>
        /// <value>Priority (high value to lower value)</value>
        public int Priority { get; set; } = 1;

        /// <summary>
        /// Column of the track table to sort results by
        /// </summary>
        /// <value>Column to sort by</value>
        public string SortColumn { get; set; }
    }
}