using System;
using System.Collections.Generic;
using Com.RFranco.Streams.State;
using static Com.Rfranco.Streams.ChangeTracking.Context.ChangeTrackingContext;

namespace Com.Rfranco.Streams.ChangeTracking.Context
{
    /// <summary>
    /// Change tracking Context
    /// </summary>
    public class ChangeTrackingContext
    {
        public enum ChangeTrackingEvent
        {
            INIT,
            DATABASEOFFSET_UPDATED,
            APPLICATIONOFFSET_UPDATED,
            PENDINGCHANGES_UPDATED,
            ERRORS_UPDATED,
            COMMITTED,
            EOF,
            MIN_OFFSET_SUPPORTED_HIGHER
        }

        public long DatabaseOffset { get; internal set; } = 0;
        public long ApplicationOffset { get; internal set; } = 0;

        public long MinimalOffsetSupported { get; internal set; } = 0;
        public long NumberOfCommits { get; internal set; } = 0;
        public long NumberOfErrors { get; internal set; } = 0;
        public long PendingChanges { get; internal set; } = 0;
        public long LastApplicationOffsetCommitted { get; internal set; } = 0;
        public DateTimeOffset StartDateTimeOffset { get; internal set; }
        public DateTimeOffset LastCommitDateTimeOffset { get; internal set; }
        public ChangeTrackingEvent LastChangeTrackingEvent { get; internal set; }
    }

    public class ChangeTrackingContextHandler : IObservable<ChangeTrackingContext>, IDisposable
    {
        private static string NAMESPACE_TRACKING = "changetracking";

        private List<IObserver<ChangeTrackingContext>> Observers;
        private ChangeTrackingContext Context;

        private StateStorage Storage;

        private string ApplicationName;

        public ChangeTrackingContextHandler(string applicationName, StateStorage storage)
        {
            ApplicationName = applicationName;
            Observers = new List<IObserver<ChangeTrackingContext>>();
            Storage = storage;
        }

        public IDisposable Subscribe(IObserver<ChangeTrackingContext> observer)
        {
            if (!Observers.Contains(observer))
            {
                Observers.Add(observer);
                observer.OnNext(Context);
            }
            return new Unsubscriber<ChangeTrackingContext>(Observers, observer);
        }

        public void Dispose()
        {
            foreach (var observer in Observers)
                observer.OnCompleted();

            Observers.Clear();
            Storage.Close();
        }

        internal void Notify()
        {
            foreach (var observer in Observers)
                observer.OnNext(Context);
        }

        public void InitContext()
        {
            Context = new ChangeTrackingContext()
            {
                ApplicationOffset = Convert.ToInt64(Storage.GetValue(GetKey())),
                StartDateTimeOffset = DateTimeOffset.UtcNow,
                LastChangeTrackingEvent = ChangeTrackingEvent.INIT
            };

            Notify();
        }
        public void InitContext(long initialApplicationOffset)
        {
            Context = new ChangeTrackingContext()
            {
                ApplicationOffset = initialApplicationOffset,
                StartDateTimeOffset = DateTimeOffset.UtcNow,
                LastChangeTrackingEvent = ChangeTrackingEvent.INIT
            };
        }

        public ChangeTrackingContext GetContext()
        {
            return Context;
        }

        internal void Commit()
        {
            Storage.Update(GetKey(), Context.ApplicationOffset);
            Context.NumberOfCommits++;
            Context.LastCommitDateTimeOffset = DateTimeOffset.UtcNow;
            Context.LastApplicationOffsetCommitted = Context.ApplicationOffset;
            Context.LastChangeTrackingEvent = ChangeTrackingEvent.COMMITTED;

            Notify();
        }

        internal void UpdateDatabaseOffset(long newDatabaseOffset)
        {
            if (Context.DatabaseOffset != newDatabaseOffset)
            {
                Context.DatabaseOffset = newDatabaseOffset;
                Context.LastChangeTrackingEvent = ChangeTrackingEvent.DATABASEOFFSET_UPDATED;
                Notify();
            }
        }

        internal void UpdateApplicationOffset()
        {
            Context.ApplicationOffset = Context.DatabaseOffset;
            Context.LastChangeTrackingEvent = ChangeTrackingEvent.APPLICATIONOFFSET_UPDATED;
            Notify();
        }

        internal void RegisterError()
        {
            Context.NumberOfErrors++;
            Context.LastChangeTrackingEvent = ChangeTrackingEvent.ERRORS_UPDATED;
            //Notify();
        }

        internal void RegisterPendingChanges(long numberOfNewChanges)
        {
            Context.PendingChanges += numberOfNewChanges;
            Context.LastChangeTrackingEvent = ChangeTrackingEvent.PENDINGCHANGES_UPDATED;
            //Notify()
        }

        internal bool isEOF()
        {
            return Context.ApplicationOffset == Context.DatabaseOffset;
        }

        internal bool HasChanges()
        {
            if (Context.DatabaseOffset <= Context.ApplicationOffset)
            {
                Context.PendingChanges = 0;
                Context.LastChangeTrackingEvent = ChangeTrackingEvent.EOF;
                return false;
            }
            
            return true;
        }

        internal bool IsMinimalOffsetSupportedHigher(long minValidVersion)
        {
            Context.MinimalOffsetSupported = minValidVersion;            
            bool isMinOffsetHigher = Context.ApplicationOffset != 0 && Context.ApplicationOffset < minValidVersion;

            if(isMinOffsetHigher)
            {
                Context.LastChangeTrackingEvent = ChangeTrackingEvent.MIN_OFFSET_SUPPORTED_HIGHER;
                Notify();
            }
            return isMinOffsetHigher;
        }

        private string GetKey()
        {
            return $"{NAMESPACE_TRACKING}-{ApplicationName}";   //TODO use : instead of -
        }
    }

    internal class Unsubscriber<ChangeTrackingContext> : IDisposable
    {
        private List<IObserver<ChangeTrackingContext>> _observers;
        private IObserver<ChangeTrackingContext> _observer;

        internal Unsubscriber(List<IObserver<ChangeTrackingContext>> observers, IObserver<ChangeTrackingContext> observer)
        {
            this._observers = observers;
            this._observer = observer;
        }

        public void Dispose()
        {
            if (_observers.Contains(_observer))
                _observers.Remove(_observer);
        }
    }
}