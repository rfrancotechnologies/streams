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
        /// IEnumerable with information related to the tables configured with change tracking
        /// </summary>
        /// <value>ChangeTrackingTableConfiguration instance white table configured</value>
        public IEnumerable<ChangeTrackingTableConfiguration> WhiteListTables { get; set; }

        public override string ToString()
        {
            string whiteTables = string.Empty;
            foreach (var whiteListTable in WhiteListTables)
            {
                if (!"".Equals(whiteTables)) whiteTables += ",";
                whiteTables += whiteListTable.ToString();
            }

            return "{\"ConnectionString\": " + ConnectionString + ",\"ApplicationName\": " + ApplicationName + ",\"PollIntervalMilliseconds\": "
            + PollIntervalMilliseconds + ",\"WhiteListTables\": [" + whiteTables + "]}";
        }
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

        public override string ToString()
        {
            return "{\"FullName\": " + FullName + ",\"PrimaryKeyColumn\": " + PrimaryKeyColumn + ",\"Priority\": " + Priority + "}";
        }
    }
}