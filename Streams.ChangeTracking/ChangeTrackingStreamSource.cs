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

namespace Com.Rfranco.Streams.ChangeTracking
{

    /// <summary>
    /// Changetracking stream source implementation
    /// </summary>
    public class ChangeTrackingStreamSource : IStreamSource<Change>
    {
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

        /// <summary>
        /// Change tracking stream constructor
        /// </summary>
        /// <param name="configuration">Changetracking configuration</param>
        public ChangeTrackingStreamSource(ChangeTrackingConfiguration configuration) : this(configuration, new MemoryStateStorage())
        {
        }

        /// <summary>
        /// Change tracking stream constructor
        /// </summary>
        /// <param name="configuration">Change tracking configuration</param>
        /// <param name="stateStorage">Change tracking state storage</param>
        public ChangeTrackingStreamSource(ChangeTrackingConfiguration configuration, StateStorage stateStorage)
        {
            this.Repository = new ChangeTrackingRepository(configuration);
            this.PollingInterval = TimeSpan.FromMilliseconds(configuration.PollIntervalMilliseconds);
            this.ContextHandler = new ChangeTrackingContextHandler(configuration.ApplicationName, stateStorage);            
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
                ContextHandler.InitContext();
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
                        foreach (var change in changes)
                            yield return change;

                        delay = TimeSpan.FromSeconds(0);
                        
                        ContextHandler.UpdateApplicationOffset();                        
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
            Stack<IEnumerable<Change>> tableChanges = new Stack<IEnumerable<Change>>();
            IEnumerable<TrackedTableInformation> changeTrackingTableInfos = Repository.GetTrackedTablesInformation(conn);
            var currentApplicationOffset = contextHandler.GetContext().ApplicationOffset;
            foreach (var changeTrackingTableInfo in changeTrackingTableInfos.OrderBy(x => x.Priority))
            {
                if (contextHandler.IsMinimalOffsetSupported(changeTrackingTableInfo.MinValidVersion))
                {
                    throw new ChangeTrackingException($"Received a minimal offset supported {changeTrackingTableInfo.MinValidVersion}"
                    + $" higher than the last processed {currentApplicationOffset} for {changeTrackingTableInfo.GetFullTableName()} table");
                }
                else
                {
                    var changes = Repository.GetOffsetChanges(conn, changeTrackingTableInfo, currentApplicationOffset);
                    tableChanges.Push(changes);
                    ContextHandler.RegisterPendingChanges(changes.Count());
                }
            }

            while (tableChanges.Any())
            {
                foreach (var change in tableChanges.Pop())
                {
                    yield return change;
                    ContextHandler.RegisterPendingChanges(-1);
                }
            }
        }

    }
}