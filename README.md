# Com.RFranco.Streams

A very simple library for stream processing.

## Usage

This library defines two basic interfaces to operate over streams, considering that a stream is an `IEnumerable<T>`:

* `IStreamSource<T>`: a source of messages. Sources are able to produce a bounded or unbounded IEnumerable of messages.
* `IStreamSink<T>`: a sink of messages. Sinks receive streams of messages and dump them into some output (like ElasticSearch, some Kafka topic or a CSV file).

An example of use of this type of this library would be:

```csharp
// Some stream source like a Kafka topic, a database changes feed or a CSV file.
IStreamSource<string> source = ...;
// Some stream sink like a Kafka topic, ElasticSearch, etc.
IStreamSink<Tuple<int, string>> sink = ...;

// Sinks are expected to work with batches of messages, for efficiency.
// When MaxBatchSize of messages is reached, the batch will be effectively dumped into the sink.
sink.MaxBatchSize = 1000;
// If the timeout is reached, even if the number of messages has not reached MaxBatchSize, the
// batch will be effectively dumped into the sink.
sink.BatchTimeout = TimeSpan.FromSeconds(10);

// Sources can be committed (that is, the can be specified that the messages up
// to a certain point have been correctly processed and don't have to be provided
// again).
// Sinks can commit sources when the successfully process a batch of messages.
sink.SetSourceToCommit(source);

// Streams can be taken from sources via the Stream(CancellationToken) call.
// Stream can be manipulated using the familiar System.Linq operations and
// then dumped into a sink.
source.Stream(CancellationToken.None).Select(word => new Tuple<int, string>(word.size(), word)).Dump(sink);
```

You need to add a dependency into your project for using specific stream sources and sinks.

### Error Handling

The sources and sinks raise events when an error occur. The user of the library is free to act over the error as necessary.

Errors provide an `IsFatal` property indicating whether the error is fatal and execution should stop. A `Reason` property provides a textual messages indicating the nature of the error:

```csharp
private void HandleErrors(StreamingError error)
{
    if (streamingError.IsFatal)
    {
        // Handle fatal errors as you consider necessary.
        cancellationTokenSource.Cancel();
    }
    log.Error(streamingError.Reason);
}

...

IStreamSource<string> source = ...;
IStreamSink<Tuple<int, string>> sink = ...;

source.OnError += HandleErrors;
sink.OnError += HandleErrors;

source.Stream(cancellationTokenSource.Token).Select(word => new Tuple<int, string>(word.size(), word)).Dump(sink);
```

### Processing Streams Without a Sink

A common use case of event streaming is to process incoming events to update your application state without explicitly dumping the stream into a sink. As stream sources provice an `IEnumerable<T>` stream, this processing is very simple:

```csharp
IStreamSource<string> source = ...;

foreach(var message in source.Stream(cancellationTokenSource.Token))
{
    // Do your processing...
}
```

### Reaching the End of a Stream

Unbound streams are those that never end, like a stream of temperature measures from a weather stations or a Kafka topic.

Some times it is very handy to be able to react to sources reaching a partial end of the underlying source. For instance, we might have a REST of GRPC service that provides information composed from the processing of messages from a source. We could want to start serving only when we have processed all the events that existed so far (so that we are not serving outdated information).

Some sources raise an event when they reach the end of the underlying source. For instance, the Kafka stream source raises an event every time it reaches the end of a topic partition.

```csharp
IStreamSource<string> source = ...;
source.OnEOF += StartGrpcServer;
source.Stream(cancellationTokenSource.Token)...
```
