using System;

namespace Com.RFranco.Streams.State.Zookeeper
{
    /// <summary>
    /// Zookeeper client options DTO
    /// </summary>
    public class ZookeeperClientOptions
    {

        /// <summary>
        /// Constructor
        /// </summary>
        public ZookeeperClientOptions()
        {
            SessionTimeoutMilliseconds = 10000;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="connectionString">Connection string where Apache Zookeeper is listening</param>
        /// <returns></returns>
        public ZookeeperClientOptions(string connectionString) : this()
        {
            if (string.IsNullOrEmpty(connectionString))
                throw new ArgumentNullException(nameof(connectionString));

            ConnectionString = connectionString;
        }

        /// <summary>
        /// Connection string Host:Port format
        /// </summary>
        /// <value></value>
        public string ConnectionString { get; set; }

        /// <summary>
        /// Session timeout in milliseconds
        /// </summary>
        /// <value></value>
        public int SessionTimeoutMilliseconds { get; set; }

        public override string ToString()
        {
            return "{\"ConnectionString\": " + ConnectionString
                + ",\"SessionTimeout\": " + SessionTimeoutMilliseconds.ToString()                 
                + "}";
        }

    }
}