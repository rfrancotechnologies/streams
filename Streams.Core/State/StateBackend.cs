namespace Com.RFranco.Streams.State
{
    /// <summary>
    /// Abstract factory to create or retrieve a state by its descriptor
    /// </summary>
    /// <typeparam name="T">Type that represents the state value</typeparam>
    public abstract class StateBackend<T>
    {
        /// <summary>
        /// Create or retrieve the state by its descriptor
        /// </summary>
        /// <param name="descriptor">State descriptor</param>
        /// <returns>The State<T> instance</returns>
        public abstract State<T> GetOrCreateState(StateDescriptor<T> descriptor);
    }
}