using System;

namespace Com.Rfranco.Streams.ChangeTracking.Models
{

    public enum ChangeOperationEnum
    {
        INSERT, UPDATE, DELETE, SYNC
    }

    /// <summary>
    /// Change information
    /// </summary>
    public class Change : IEquatable<Change>, IComparable
    {
        public string PrimaryKeyValue { get; set; }

        private string _ChangeOperation { get; set; }

        public ChangeOperationEnum ChangeOperation
        {
            get
            {
                if (null == _ChangeOperation) return ChangeOperationEnum.SYNC;
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

        public dynamic SortColumn { get; set; }

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

        public int CompareTo(object o)
        {
            var other = o as Change;
            if(!SortColumn.GetType().Equals(other.SortColumn.GetType()))
                throw new ArgumentException("The SortColumn of all tables should be of the same SQL type");

            if(typeof(IComparable).IsAssignableFrom(SortColumn.GetType()))
                return SortColumn.CompareTo(other.SortColumn);

            throw new ArgumentException("The SortColumn type not implements the 'CompareTo' method. " + 
                "An expecific comparer for this data type shold be implemented.");
        }
    }
}