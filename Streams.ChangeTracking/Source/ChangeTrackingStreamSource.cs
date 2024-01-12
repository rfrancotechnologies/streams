using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Com.Rfranco.Streams.ChangeTracking.Config;
using Com.Rfranco.Streams.ChangeTracking.Exceptions;
using Com.Rfranco.Streams.ChangeTracking.Models;
using Com.RFranco.Streams;
using Com.RFranco.Streams.State;
using Com.Rfranco.Streams.ChangeTracking.Repositories;
using System.Data;
using System.Linq;
using Com.Rfranco.Streams.ChangeTracking.Context;

namespace Com.Rfranco.Streams.ChangeTracking.Source
{
    /// <summary>
    /// Changetracking stream source implementation
    /// </summary>
    public class ChangeTrackingStreamSource : IChangeTrackingStreamSource
    {
        /// <summary>
        /// Enable/Disable the stream source commit of the published messages.
        /// </summary>
        public bool CommitEnable { get; set; } = true;
        private long? InitialChangeTable {get; set;}
        
        /// <summary>
        /// Action to be performed when EOF event is detected
        /// </summary>
        public event Action OnEOF;

        /// <summary>
        /// Action to be performed when and error is detected
        /// </summary>
        public event Action<StreamingError> OnError;

        /// <summary>
        /// Engine
        /// </summary>
        private IChangeTrackingRepository Repository;

        /// <summary>
        /// Context handler
        /// </summary>
        private ChangeTrackingContextHandler ContextHandler;

        /// <summary>
        /// Polling interval defined
        /// </summary>
        private TimeSpan PollingInterval;

        private ChangeTrackingConfiguration changeTrackingConfig;

        /// <summary>
        /// Change tracking stream constructor
        /// </summary>
        /// <param name="configuration">Change tracking configuration</param>
        /// <param name="stateStorage">Change tracking state storage</param>
        public ChangeTrackingStreamSource(ChangeTrackingConfiguration configuration, StateStorage stateStorage = null)
        {
            this.Repository = new ChangeTrackingRepository(configuration);
            this.PollingInterval = TimeSpan.FromMilliseconds(configuration.PollIntervalMilliseconds);
            this.ContextHandler = new ChangeTrackingContextHandler(configuration.ApplicationName, stateStorage ?? new MemoryStateStorage());
            changeTrackingConfig = configuration;
        }

        /// <summary>
        /// Retrieves the stream of messages from the change tracking source.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns>IEnumerable of messages of type Change</returns>
        public IEnumerable<Change> Stream(CancellationToken cancellationToken)
        {
            Stopwatch processTime = Stopwatch.StartNew();
            IEnumerable<Change> changes = null;
            TimeSpan delay = TimeSpan.FromSeconds(0);
            bool alreadyInvokeEOF = false;

            try
            {
                if(CommitEnable)
                    ContextHandler.InitContext();
                else
                    ContextHandler.InitContext(InitialChangeTable == null ? 0 : (long)InitialChangeTable);
            }
            catch (Exception ex)
            {
                OnError?.Invoke(new StreamingError { IsFatal = true, Reason = ex.Message });
            }

            while (!cancellationToken.IsCancellationRequested)
            {
                using (var conn = Repository.CreateConnection())
                {
                    try
                    {
                        changes = null;

                        ContextHandler.UpdateDatabaseOffset(Repository.GetDatabaseOffset(conn));
                        if (ContextHandler.HasChanges())
                        {
                            changes = GetChanges(conn, ContextHandler);
                            alreadyInvokeEOF = false;
                        }
                        else if (ContextHandler.isEOF() && !alreadyInvokeEOF)
                        {
                            OnEOF?.Invoke();
                            alreadyInvokeEOF = true;
                        }
                    }
                    catch (Exception cte)
                    {
                        OnError?.Invoke(new StreamingError { IsFatal = cte is ChangeTrackingException, Reason = cte.Message });
                        ContextHandler.RegisterError();

                        delay = delay.Add(TimeSpan.FromSeconds(5));
                        if (delay > TimeSpan.FromSeconds(15))
                            delay = TimeSpan.FromSeconds(15);
                    }

                    if (null != changes)
                    {
                        bool mustSync = false;
                        foreach (var change in changes)
                            if(change.ChangeOperation.Equals(ChangeOperationEnum.SYNC))
                                mustSync = true;
                            else
                                yield return change;

                        delay = TimeSpan.FromSeconds(0);

                        ContextHandler.UpdateApplicationOffset();
                        if(mustSync && CommitEnable)
                            Commit();
                    }
                }

                processTime.Stop();

                var sleep = (delay + PollingInterval) - processTime.Elapsed;
                if (sleep.TotalMilliseconds > 0)
                    Thread.Sleep(sleep);

                processTime.Restart();
            }

            ContextHandler.Dispose();
        }

        /// <summary>
        /// Commit
        /// </summary>
        public void Commit()
        {
            if(CommitEnable)
                ContextHandler.Commit();
        }

        /// <summary>
        /// Get context handler
        /// </summary>
        /// <returns>Returns context handler</returns>
        public ChangeTrackingContextHandler GetContextHandler()
        {
            return ContextHandler;
        }

        /// <summary>
        /// Read and process all the table changes from the provided tables.
        /// 
        /// Table changes are first read from lower priority to higher priority tables, and subsequently processed
        /// from higher priority tables to lower priority ones. This is made in order to prevent processing changes 
        /// from dependent entities that haven't yet been processed (keeping consistency with dependent entities and
        /// the generated events). 
        /// For instance, assuming we are processing player wallets and player wallet transactions (wallets have higher
        /// priority than transactions), to prevent reading player wallet transactions from wallets that have not yet been
        /// read we first read changes in player wallet transactions, then we read changes in wallets, then we process the
        /// changes in wallets and finally we process the change in transactions.
        /// </summary>
        /// <param name="conn">The database connection.</param>
        /// <param name="ChangeTrackingContextHandler">Context handler</param>
        private IEnumerable<Change> GetChanges(IDbConnection conn, ChangeTrackingContextHandler contextHandler)
        {
            var tableChanges = new List<Change>();
            var changeTrackingTableInfos = Repository.GetTrackedTablesInformation(conn);
            
            var currentApplicationOffset = contextHandler.GetContext().ApplicationOffset;
            var dbOffset = contextHandler.GetContext().DatabaseOffset;

            foreach (var changeTrackingTableInfo in changeTrackingTableInfos.OrderBy(x => x.Priority))
            {
                if (!contextHandler.IsMinimalOffsetSupportedHigher(changeTrackingTableInfo.MinValidVersion))                
                {
                    var changes = Repository.GetOffsetChanges(conn, changeTrackingTableInfo, currentApplicationOffset, dbOffset);
                    if(changes.Count() != 0)
                    {
                        tableChanges.AddRange(changes);
                        ContextHandler.RegisterPendingChanges(changes.Count());
                    }
                }
            }

            if(changeTrackingConfig.CustomSortEnable)
                tableChanges.Sort((x,y) => y.CompareTo(x));

            if (!tableChanges.Any())
                yield return new Change {}; //  SYNC
            else
            {
                for(int i=tableChanges.Count-1; i>=0; i--)
                {
                    yield return tableChanges[i];
                    tableChanges.RemoveAt(i);
                }
            }
        }

        public void SetInitialChangeTable(long initialChangeTable)
        {
            if(CommitEnable)
                throw new ArgumentException("The Initial Change Table can not been setted if " +
                    "'CommitEnable' property was setted to true.");

            InitialChangeTable = initialChangeTable;
        }
    }
}