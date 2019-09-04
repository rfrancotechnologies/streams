namespace Com.Rfranco.Streams.ChangeTracking.Models
{
    /// <summary>
    /// Change tracking table information
    /// </summary>
    public class TrackedTableInformation
    {

        public string SchemaName { get; set; }

        public string TableName { get; set; }

        public int ObjectId { get; set; }

        public bool Enabled { get; set; }

        public long MinValidVersion { get; set; }

        public long BeginVersion { get; set; }

        public long CleanUpVersion { get; set; }

        public string PrimaryKeyColumn { get; set; }

        public int Priority { get; set; }

        public string GetFullTableName()
        {
            return SchemaName + "." + TableName;
        }


        public override string ToString()
        {
            return "{\"SchemaName\": " + SchemaName + ",\"TableName\": " + TableName + ",\"PrimaryKeyColumn\": " + PrimaryKeyColumn + ",\"ObjectId\": " + ObjectId
                + ",\"Enabled\": " + Enabled + ",\"MinValidVersion\": " + MinValidVersion + ",\"BeginVersion\": " + BeginVersion + ",\"CleanUpVersion\" : " + CleanUpVersion + "}";
        }
    }
}