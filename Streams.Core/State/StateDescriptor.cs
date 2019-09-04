namespace Com.RFranco.Streams.State
{

    /// <summary>
    /// State descriptor
    /// </summary>
    /// <typeparam name="T">Type that represents the state</typeparam>
    public class StateDescriptor<T>
    {
        /// <summary>
        /// Namespace of the state
        /// </summary>
        public readonly string Namespace;

        /// <summary>
        /// Name of the state
        /// </summary>
        public readonly string Name;

        /// <summary>
        /// Serializer / Deserializer of the state value
        /// </summary>
        public TypeSerializer<T> Serializer;

        /// <summary>
        /// Default value of the state
        /// </summary>
        public T DefaultValue;

        /// <summary>
        /// State descriptor constructor
        /// </summary>
        /// <param name="Namespace">Namespace of the state</param>
        /// <param name="name">Name of the state</param>
        /// <param name="serializer">Instance of serializer used to serialize / deserialize the state value</param>
        /// <param name="defaultValue">Default value of the state</param>
        public StateDescriptor(string Namespace, string name, TypeSerializer<T> serializer, T defaultValue)
        {
            Name = name;
            this.Namespace = Namespace;
            Serializer = serializer;
            DefaultValue = defaultValue;
        }
    }
}