using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Threading;
using Com.Rfranco.Streams.ChangeTracking.Config;
using Com.Rfranco.Streams.ChangeTracking.Exceptions;
using Com.Rfranco.Streams.ChangeTracking.Models;
using Com.RFranco.Streams;
using Com.RFranco.Streams.State;
using System.Data.SqlClient;

namespace Com.Rfranco.Streams.ChangeTracking
{

    /// <summary>
    /// Changetracking stream source implementation
    /// </summary>
    public class ChangeTrackingStreamSource : IStreamSource<Change>
    {

        /// <summary>
        /// Default changetracking state namespace
        /// </summary>
        private readonly string APPLICATION_OFFSET_STATE_NAMESPACE = "changetracking";

        /// <summary>
        /// Action to be performed when EOF event is detected
        /// </summary>
        public event Action OnEOF;

        /// <summary>
        /// Flag to detect if EOF action has been already performed or not
        /// </summary>
        private bool EOFActionAlreadyThrown = false;

        /// <summary>
        /// Action to be performed when and error is detected
        /// </summary>
        public event Action<StreamingError> OnError;

        /// <summary>
        /// ChangeTracking configuration
        /// </summary>
        private ChangeTrackingConfiguration Configuration;

        /// <summary>
        /// Change tracking manager instance
        /// </summary>
        private IChangeTrackingManager ChangeTrackingManager;

        /// <summary>
        /// Changetracking state factory to create or retrieve the state related
        /// </summary>
        private StateBackend<long?> ChangeTrackingStateFactory;

        /// <summary>
        /// Changetracking state
        /// </summary>
        State<long?> ChangeTrackingState;

        /// <summary>
        /// Change tracking stream constructor
        /// </summary>
        /// <param name="configuration">Changetracking configuration</param>
        /// <typeparam name="long?"></typeparam>
        /// <returns></returns>
        public ChangeTrackingStreamSource(ChangeTrackingConfiguration configuration) : this(configuration, new MemoryStateBackend<long?>())
        {
        }

        /// <summary>
        /// Change tracking stream constructor
        /// </summary>
        /// <param name="configuration">Change tracking configuration</param>
        /// <param name="stateBackend">ChangeTracking state factory</param>
        /// <returns></returns>
        public ChangeTrackingStreamSource(ChangeTrackingConfiguration configuration, StateBackend<long?> stateBackend) : this(configuration, stateBackend, new ChangeTrackingManager(configuration))
        {
        }

        /// <summary>
        /// Change tracking stream constructor
        /// </summary>
        /// <param name="configuration">Change tracking configuration</param>
        /// <param name="changeTrackingEngine">Change tracking engine instance</param>
        /// <param name="stateBackend">Change tracking state factory</param>
        public ChangeTrackingStreamSource(ChangeTrackingConfiguration configuration, StateBackend<long?> stateBackend, IChangeTrackingManager changeTrackingEngine)
        {
            this.Configuration = configuration;
            this.ChangeTrackingManager = changeTrackingEngine;
            this.ChangeTrackingStateFactory = stateBackend;
            this.ChangeTrackingState = stateBackend.GetOrCreateState(new StateDescriptor<long?>(APPLICATION_OFFSET_STATE_NAMESPACE, configuration.ApplicationName, Serializers.NullableLongSerializer, null));
        }

        /// <summary>
        /// Retrieves the stream of messages from the change tracking source.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns>IEnumerable of messages of type Change</returns>
        public IEnumerable<Change> Stream(CancellationToken cancellationToken)
        {
            long databaseOffset = 0;
            TimeSpan pollingInterval = TimeSpan.FromMilliseconds(Configuration.PollIntervalMilliseconds);
            Stopwatch processTime = Stopwatch.StartNew();
            IEnumerable<Change> changes = null;
            long? applicationOffset = ChangeTrackingState.Value();
            while (!cancellationToken.IsCancellationRequested)
            {
                using (IDbConnection conn = CreateConnection())
                {
                    try
                    {
                        processTime.Restart();
                        databaseOffset = ChangeTrackingManager.GetDatabaseOffset(conn);
                        
                        if (!applicationOffset.HasValue|| databaseOffset > applicationOffset)
                        {
                            changes = ChangeTrackingManager.GetChanges(conn, applicationOffset);
                            EOFActionAlreadyThrown = false;
                        }
                        else
                        {
                            if (!EOFActionAlreadyThrown)
                            {
                                OnEOF?.Invoke();
                                EOFActionAlreadyThrown = true;
                                changes = null;
                            }
                        }
                    }
                    catch (Exception cte)
                    {
                        OnError?.Invoke(new StreamingError { IsFatal = cte is ChangeTrackingException, Reason = cte.Message });
                    }

                    if (null != changes)
                    {
                        foreach (var change in changes)
                        {
                            yield return change;
                        }
                        ChangeTrackingState.Update(databaseOffset);
                        applicationOffset = databaseOffset;
                    }

                    processTime.Stop();

                    var sleep = pollingInterval - processTime.Elapsed;
                    if (sleep.TotalMilliseconds > 0)
                        Thread.Sleep(sleep);
                }
            }

            ChangeTrackingState.Close();
        }

        /// <summary>
        /// Do nothing
        /// </summary>
        public void Commit()
        {
        }

        /// <summary>
        /// Create a Database connection
        /// </summary>
        /// <returns>Database connection</returns>
        private IDbConnection CreateConnection()
        {
            SqlConnection connection = new SqlConnection(Configuration.ConnectionString);
            if (connection.State != ConnectionState.Open)
                connection.Open();
            return connection;
        }
    }
}