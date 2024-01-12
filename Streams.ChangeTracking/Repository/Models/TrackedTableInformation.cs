namespace Com.Rfranco.Streams.ChangeTracking.Models
{

    public class TrackedTableInformation
    {

        public string SchemaName { get; set; }

        public string TableName { get; set; }

        public string PrimaryKeyColumn { get; set; }

        public string SortColumn { get; set;}

        public int Priority { get; set; }

        public long MinValidVersion { get; set; }

        public string GetFullTableName()
        {
            return SchemaName + "." + TableName;
        }
    }
}