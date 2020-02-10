using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Com.Rfranco.Streams.ChangeTracking;
using Com.Rfranco.Streams.ChangeTracking.Config;
using Com.RFranco.Streams.State.Redis;

namespace Com.Rfranco.Iris.Example.Tracking
{
    class Program
    {
        public static void Main(string[] args)
        {
            var exitEvent = new ManualResetEvent(false);
            Console.CancelKeyPress += (sender, ev) =>
            {
                exitEvent.Set();
                ev.Cancel = true;
            };

            
            try
            {
                CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();

                var backgroundTasks = new List<Task>();
                backgroundTasks.Add(CreateTask().Run(cancellationTokenSource.Token));
                exitEvent.WaitOne();
                cancellationTokenSource.Cancel();
                Task.WaitAll(backgroundTasks.ToArray());
            }
            catch (Exception ex)
            {
                Console.WriteLine("An error occurred while starting up the Change Tracking Events Extractor job.", ex);
                Environment.Exit(-2);
            }
        }
        
        private static TrackingTask CreateTask()
        {
            var redisConfiguration = new RedisConfiguration()
            {
                Nodes = "localhost:6379"
            };

            var trackingConfiguration = new ChangeTrackingConfiguration()
            {
                    PollIntervalMilliseconds = 10000,
                    // TODO Complete the configuration
                    ConnectionString = "",                    
                    WhiteListTables = new List<ChangeTrackingTableConfiguration>(){}
            };

            var changeTrackingSource = new ChangeTrackingStreamSource(trackingConfiguration, new RedisStateStorage(redisConfiguration));

            var monitor = new ChangeTrackingMonitor();
            monitor.Subscribe(changeTrackingSource.GetContextHandler());
            
            return new TrackingTask(changeTrackingSource);
        }
    }
}
