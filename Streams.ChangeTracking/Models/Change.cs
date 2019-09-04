namespace Com.Rfranco.Streams.ChangeTracking.Models
{

    public enum ChangeOperationEnum
    {
        INSERT, UPDATE, DELETE
    }

    /// <summary>
    /// Change information
    /// </summary>
    public class Change
    {
        public string PrimaryKeyValue { get; set; }

        public byte[] ChangeColumns { get; set; }

        public byte[] ChangeContext { get; set; }

        private string _ChangeOperation { get; set; }

        public ChangeOperationEnum ChangeOperation
        {
            get
            {
                switch (_ChangeOperation)
                {
                    case "D": return ChangeOperationEnum.DELETE;
                    case "U": return ChangeOperationEnum.UPDATE;
                    default: return ChangeOperationEnum.INSERT;

                }
            }
            
        }

        public long ChangeVersion { get; set; }

        public long ChangeCreationVersion { get; set; }

        public string TableFullName { get; set; }

        public bool IsInitial() {
            return string.IsNullOrEmpty(PrimaryKeyValue);
        }


        
        public override string ToString()
        {
            return "{\"PrimaryKeyValue\": " + PrimaryKeyValue + ",\"ChangeColumns\": " + ChangeColumns != null ? System.Text.Encoding.UTF8.GetString(ChangeColumns) : "" + ",\"ChangeContext\": " + ChangeContext != null ? System.Text.Encoding.UTF8.GetString(ChangeContext) : ""
                + ",\"TableFullName\": " + TableFullName + ",\"ChangeOperation\": " + ChangeOperation.ToString() + ",\"ChangeVersion\": " + ChangeVersion + ",\"ChangeCreationVersion\": " + ChangeCreationVersion + "}";
        }

    }
}