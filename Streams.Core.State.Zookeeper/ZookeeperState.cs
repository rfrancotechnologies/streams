using System;
using System.Threading.Tasks;
using org.apache.zookeeper;

namespace Com.RFranco.Streams.State.Zookeeper
{
    /// <summary>
    /// State implementation based on Zookeeper
    /// </summary>
    public class ZookeeperState<T> : State<T>
    {
        /// <summary>
        /// Zookeeper client
        /// </summary>
        private ZooKeeper ZooKeeperClient;

        /// <summary>
        /// Zookeeper node path
        /// </summary>
        private string Path;


        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="descriptor">State descriptor</param>
        /// <param name="configuration">Zookeeper client options</param>
        /// <returns></returns>
        public ZookeeperState(StateDescriptor<T> descriptor, ZookeeperClientOptions configuration) :
            this(descriptor, new ZooKeeper(configuration.ConnectionString, (int)configuration.SessionTimeoutMilliseconds, new ZooKeeperWatcher()))
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="descriptor">State descriptor</param>
        /// <param name="zookeeperClient">Zookeeper client instance</param>
        /// <returns></returns>
        public ZookeeperState(StateDescriptor<T> descriptor, ZooKeeper zookeeperClient) : base(descriptor)
        {
            this.ZooKeeperClient = zookeeperClient;
            Path = "/" + Descriptor.Namespace + "/" + Descriptor.Name;
        }


        /// <summary>
        /// Get state value
        /// </summary>
        /// <returns>State value</returns>
        public override T Value()
        {
            T State = Descriptor.DefaultValue;

            try
            {
                var offsetTask = ZooKeeperClient.getDataAsync(Path, true);
                offsetTask.Wait();
                State = Descriptor.Serializer.Deserialize(offsetTask.Result.Data);
            }
            catch (Exception)
            {
            }

            return State;
        }

        /// <summary>
        /// Update state value
        /// </summary>
        /// <param name="newState">New state value</param>
        public override void Update(T newState)
        {
            try
            {
                ZooKeeperClient.setDataAsync(Path, Descriptor.Serializer.Serialize(newState)).Wait();
            }
            catch (AggregateException ex)
            {
                KeeperException keeperException = ex.InnerException as KeeperException;
                if (keeperException != null && keeperException.getCode() == KeeperException.Code.NONODE)
                {
                    CreateIfNotExist(Path, Descriptor.Serializer.Serialize(newState));
                }
            }
        }

        /// <summary>
        /// Close Zookeeper client
        /// </summary>
        public override void Close()
        {
            ZooKeeperClient.closeAsync();
        }

        /// <summary>
        /// Delete zookeeper node used to store the state
        /// </summary>
        public override void Clear()
        {
            ZooKeeperClient.deleteAsync(Path).Wait();
        }

        /// <summary>
        /// Create of not exist the nodes of the path
        /// </summary>
        /// <param name="path">Node Path</param>
        /// <param name="data">Array of bytes that represents the value to be stored</param>
        private void CreateIfNotExist(string path, byte[] data)
        {
            try
            {
                var createTask = ZooKeeperClient.createAsync(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                createTask.Wait();
            }
            catch (AggregateException ex)
            {
                KeeperException keeperException = ex.InnerException as KeeperException;
                if (keeperException != null && keeperException.getCode() == KeeperException.Code.NONODE)
                {
                    int pos = path.LastIndexOf("/");
                    String parentPath = path.Substring(0, pos);
                    CreateIfNotExist(parentPath, new byte[0]);
                    CreateIfNotExist(path, data);
                }
                else if (keeperException == null || keeperException != null && keeperException.getCode() != KeeperException.Code.NODEEXISTS)
                {
                    throw ex;
                }
            }
        }
    }

    /// <summary>
    /// A dummy sample of Zookeeper Watcher
    /// </summary>
    public class ZooKeeperWatcher : Watcher
    {
        /// <summary>
        /// Manage the eventes detected by the watcher.null In this case, do nothing
        /// </summary>
        public override Task process(WatchedEvent @event)
        {
            return new Task(() => { });
        }
    }
}