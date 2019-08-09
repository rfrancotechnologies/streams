namespace Streams
{
    /// <summary>
    /// An error occured during the streaming process.
    /// </summary>
    public class StreamingError
    {
        /// <summary>
        /// Whether the error is fatal or not. Fatal errors should stop the execution.
        /// </summary>
        public bool IsFatal { get; set; }

        /// <summary>
        /// Textual message indicating the nature of the error.
        /// </summary>
        public string Reason { get; set; }
    }
}