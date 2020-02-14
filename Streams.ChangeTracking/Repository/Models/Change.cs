using System;

namespace Com.Rfranco.Streams.ChangeTracking.Models
{

    public enum ChangeOperationEnum
    {
        INSERT, UPDATE, DELETE
    }

    /// <summary>
    /// Change information
    /// </summary>
    public class Change : IEquatable<Change>
    {
        public string PrimaryKeyValue { get; set; }

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

        public string TableFullName { get; set; }

        public long ChangeVersion { get; set; }

        public bool Equals(Change other)
        {
            if (Object.ReferenceEquals(other, null)) return false;

            if (Object.ReferenceEquals(this, other)) return true;

            return PrimaryKeyValue.Equals(other.PrimaryKeyValue) && _ChangeOperation.Equals(other._ChangeOperation);    //  TODO
        }

        public override int GetHashCode()
        {
            int hashPrimaryKeyValue = PrimaryKeyValue.GetHashCode();
            int hashChangeOperation = _ChangeOperation.GetHashCode();

            return hashPrimaryKeyValue ^ hashChangeOperation;
        }
    }
}