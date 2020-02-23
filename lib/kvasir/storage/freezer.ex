defmodule Kvasir.Storage.Freezer do
  defmacro __using__(opts \\ []) do
    source = opts[:source] || raise "Need to set EventSource."
    topic = opts[:topic]
    {storage, storage_name} = opts[:storage]

    quote do
      @doc false
      @spec child_spec(Keyword.t()) :: map
      def child_spec(_opts \\ []) do
        %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, []}
        }
      end

      @doc false
      @spec start_link :: {:ok, pid} | {:error, term}
      def start_link do
        unquote(source).subscribe(unquote(topic), __MODULE__, group: inspect(__MODULE__))
      end

      @doc false
      @spec init(Kvasir.Topic.t(), non_neg_integer, term) :: {:ok, Kvasir.Topic.t()}
      def init(topic, _partition, _opts), do: {:ok, topic}

      @doc false
      @spec event(Kvasir.Event.t(), Kvasir.Topic.t()) :: :ok | {:error, atom}
      def event(event, topic), do: unquote(storage).freeze(unquote(storage_name), topic, event)
    end
  end
end
