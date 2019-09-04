using org.apache.zookeeper;

namespace Com.RFranco.Streams.State.Zookeeper
{
    /// <summary>
    /// Zookeeper state factory
    /// </summary>
    /// <typeparam name="T">Type to represent the state value</typeparam>
    public class ZookeeperStateBackend<T> : StateBackend<T>
    {

        /// <summary>
        /// Zookeeper client options
        /// </summary>
        private ZookeeperClientOptions Configuration;

        /// <summary>
        /// Zookeeper client instance
        /// </summary>
        private ZooKeeper ZooKeeperClient;

        /// <summary>
        /// ZookeeperState Constructor from Zookeeper client options
        /// </summary>
        /// <param name="configuration">Zookeeper client options</param>
        public ZookeeperStateBackend(ZookeeperClientOptions configuration)
        {
            Configuration = configuration;
        }

        /// <summary>
        /// ZookeeperState contructor from zookeeper client instance
        /// </summary>
        /// <param name="zooKeeperClient"></param>
        public ZookeeperStateBackend(ZooKeeper zooKeeperClient)
        {
            ZooKeeperClient = zooKeeperClient;
        }

        /// <summary>
        /// Create or retrieve a zookeeper state by its descriptor
        /// </summary>
        /// <param name="descriptor">State descriptor</param>
        /// <returns>Zookeeper State instance</returns>
        public override State<T> GetOrCreateState(StateDescriptor<T> descriptor)
        {
            return (ZooKeeperClient != null) ? new ZookeeperState<T>(descriptor, ZooKeeperClient) : new ZookeeperState<T>(descriptor, Configuration);
        }
    }
}