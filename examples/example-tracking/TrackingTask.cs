using System;
using System.Collections.Generic;
using System.Linq;
using Com.Rfranco.Streams.ChangeTracking.Models;
using System.Threading;
using System.Threading.Tasks;
using Com.RFranco.Streams;
using Com.Rfranco.Streams.ChangeTracking.Exceptions;

namespace Com.Rfranco.Iris.Example.Tracking
{
    public class TrackingTask
    {
        IStreamSource<Change> Source;
        
        public TrackingTask(IStreamSource<Change> source)
        {
            this.Source = source;            
        }


        public Task Run(CancellationToken cancellationToken)
        {

            return Task.Factory.StartNew(() =>
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        Source.Stream(cancellationToken).SelectMany((change) => ProcessChange(change));
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Something went wrong extracting data", e);
                        if (e is ChangeTrackingException)
                        {
                            Environment.Exit(-1);
                        }
                    }
                }
            });
        }



        private IEnumerable<KeyValuePair<string, object>> ProcessChange(Change change)
        {
            Console.WriteLine($"Change on {change.TableFullName} table, {change.PrimaryKeyValue} row of type {change.ChangeOperation.ToString()} ");
            yield break;
        }        
    }
    
}