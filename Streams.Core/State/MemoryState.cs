namespace Com.RFranco.Streams.State
{
    /// <summary>
    /// In memory state implementation
    /// </summary>
    /// <typeparam name="T">Type of the state value</typeparam>
    public class MemoryState<T> : State<T>
    {
        /// <summary>
        /// State value
        /// </summary>
        protected T State;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="descriptor">State descriptor</param>
        /// <returns>MemoryState instance</returns>
        public MemoryState(StateDescriptor<T> descriptor) : this(descriptor, descriptor.DefaultValue)
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="descriptor">State descriptor</param>
        /// <param name="state">State value</param>
        /// <returns>MemoryState instance</returns>
        public MemoryState(StateDescriptor<T> descriptor, T state) : base(descriptor)
        {
            State = state;
        }

        /// <summary>
        /// Retrieve the value of the in memory state
        /// </summary>
        /// <returns>Value</returns>
        public override T Value()
        {
            return State;
        }

        /// <summary>
        /// Update the in memory state value
        /// </summary>
        /// <param name="newState">New state value</param>
        public override void Update(T newState)
        {
            State = newState;
        }

        /// <summary>
        /// Clear the in memory state
        /// </summary>
        public override void Clear()
        {
            State = Descriptor.DefaultValue;
        }

        /// <summary>
        /// Close the in memory state
        /// </summary>
        public override void Close()
        {
            Clear();
        }
    }

    /// <summary>
    /// In Memory state factory
    /// </summary>
    /// <typeparam name="T">Type of the state value</typeparam>
    public class MemoryStateBackend<T> : StateBackend<T>
    {
        /// <summary>
        /// Create or retrive an in memory state
        /// </summary>
        /// <param name="descriptor">State descriptor</param>
        /// <returns>An instance of State<T></returns>
        public override State<T> GetOrCreateState(StateDescriptor<T> descriptor)
        {
            return new MemoryState<T>(descriptor);
        }
    }
}