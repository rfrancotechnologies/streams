using System;
using Com.Rfranco.Streams.ChangeTracking;

namespace Com.Rfranco.Iris.Example.Tracking
{
    public class ChangeTrackingMonitor : IObserver<ChangeTrackingContext>
    {
        private IDisposable Cancellation;

        private ChangeTrackingContext CurrentContext;


        public void Subscribe(ChangeTrackingContextHandler provider)
        {
            Cancellation = provider.Subscribe(this);
        }

        public void Unsubscribe()
        {
            Cancellation.Dispose();
            CurrentContext = null;
        }


        public void OnCompleted()
        {
            if (null != CurrentContext)
                Print();
        }

        public void OnError(Exception error)
        {
        }

        public void OnNext(ChangeTrackingContext context)
        {
            CurrentContext = context;
            if (null != CurrentContext)
                Print();
        }

        private void Print()
        {
            var info = "\"DatabaseOffset\":" + CurrentContext.DatabaseOffset
            + ",\"ApplicationOffset\": " + CurrentContext.ApplicationOffset
            + ",\"LastEvent\": " + CurrentContext.LastChangeTrackingEvent.ToString()
            + ",\"NumberOfCommits\": " + CurrentContext.NumberOfCommits
            + ",\"NumberOfErrors\": " + CurrentContext.NumberOfErrors
            + ",\"PendingChanges\": " + CurrentContext.PendingChanges
            + ",\"LastApplicationOffsetCommitted\": " + CurrentContext.LastApplicationOffsetCommitted
            + ",\"StartDateTimeOffset\": \"" + CurrentContext.StartDateTimeOffset.ToString() + "\""
            + ",\"LastCommitDateTimeOffset\": \"" + ((CurrentContext.LastCommitDateTimeOffset != default(DateTimeOffset)) ? CurrentContext.LastCommitDateTimeOffset.ToString() : "null") + "\"";

            Console.WriteLine($"Current context of the ChangeTracking: {info}");
        }
    }
}
