using System;
using System.Threading.Tasks;
using org.apache.zookeeper;

namespace Com.RFranco.Streams.State.Zookeeper
{

    public class ZookeeperStateStorage : StateStorage
    {

        /// <summary>
        /// Zookeeper client
        /// </summary>
        private ZooKeeper ZooKeeperClient;

        public ZookeeperStateStorage(ZookeeperClientOptions options)
        {
            ZooKeeperClient = new ZooKeeper(options.ConnectionString, options.SessionTimeoutMilliseconds, new ZooKeeperWatcher());            
        }

        /// <summary>
        /// Get the value of the state
        /// </summary>
        /// <param name="key">Key to identify the state. Must start with / </param>
        /// <returns></returns>
        public override object GetValue(string key)
        {
            object state = null;

            try
            {
                var readTask = ZooKeeperClient.getDataAsync(key, true);
                readTask.Wait();
                state = Deserialize(readTask.Result.Data);
            }
            catch (Exception ex)
            {
                KeeperException keeperException = ex.InnerException as KeeperException;
                if (keeperException != null && keeperException.getCode() == KeeperException.Code.CONNECTIONLOSS)
                {
                    throw ex;
                }
            }

            return state;
        }

        /// <summary>
        /// Update the state
        /// </summary>
        /// <param name="key">Key to identify the state. Must start with / </param>
        /// <param name="newValue"></param>
        public override void Update(string key, object newValue)
        {
            try
            {
                ZooKeeperClient.setDataAsync(key, Serialize(newValue)).Wait();
            }
            catch (AggregateException ex)
            {
                KeeperException keeperException = ex.InnerException as KeeperException;
                if (keeperException != null && keeperException.getCode() == KeeperException.Code.NONODE)
                {
                    CreateIfNotExist(key, Serialize(newValue));
                }
            }
        }

        /// <summary>
        /// Clear the state
        /// </summary>
        /// <param name="key">Key to identify the state. Must start with / </param>
        public override void Clear(string key)
        {
            ZooKeeperClient.deleteAsync(key).Wait();
        }

        public override void Close()
        {
            ZooKeeperClient.closeAsync();
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
}