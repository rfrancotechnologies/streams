namespace Com.RFranco.Streams.State
{
    /// <summary>
    /// Abstract State representation
    /// </summary>
    /// <typeparam name="T">Type that represents the state value</typeparam>
    public abstract class State<T>
    {
        /// <summary>
        /// State descriptor
        /// </summary>
        protected StateDescriptor<T> Descriptor;

        /// <summary>
        /// State Constructor
        /// </summary>
        /// <param name="descriptor">State descriptor</param>
        public State(StateDescriptor<T> descriptor)
        {
            Descriptor = descriptor;
        }

        /// <summary>
        /// Clear the state
        /// </summary>
        public abstract void Clear();

        /// <summary>
        /// Retrieve the value of the state
        /// </summary>
        /// <returns></returns>
        public abstract T Value();

        /// <summary>
        /// Update the state value
        /// </summary>
        /// <param name="value">New Value</param>
        public abstract void Update(T value);

        /// <summary>
        /// Close the state
        /// </summary>
        public abstract void Close();
    }
}