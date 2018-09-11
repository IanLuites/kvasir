defmodule Kvasir.Client do
  defmacro __using__(opts \\ []) do
    otp = Keyword.fetch!(opts, :otp_app)

    quote bind_quoted: [otp: otp] do
      use GenServer
      import Kvasir.Client.{Consumer, Info, Producer}
      @client Module.concat([__MODULE__, Client])

      @spec start_link(Keyword.t()) :: Supervisor.child_spec()
      def start_link(opts \\ []) do
        unless elem(:application.ensure_all_started(:brod), 0) == :ok do
          raise "Can not start the required dependency: :brod"
        end

        config = Application.get_env(unquote(otp), __MODULE__, [])
        events = Kvasir.Event.Discovery.discover(config[:events])
        config = Keyword.put(config, :events, events)

        GenServer.start_link(__MODULE__, %{config: config}, name: __MODULE__)
      end

      def init(state) do
        :brod.start_client(state.config[:kafka], @client)

        Enum.each(state.config[:topics] || [], &producer/1)

        {:ok, state}
      end

      @spec child_spec(Keyword.t()) :: map
      def child_spec(opts \\ []) do
        %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [opts]},
          type: :supervisor
        }
      end

      ### Consumer ###

      def consume(topic, callback, opts \\ []) do
        with %{config: config} <- :sys.get_state(__MODULE__) do
          base = [events: config[:events] || []]
          consume(config[:kafka], topic, callback, Keyword.merge(base, opts))
        end
      end

      def stream(topic, opts \\ []) do
        with %{config: config} <- :sys.get_state(__MODULE__) do
          base = [events: config[:events] || []]
          stream(config[:kafka], topic, Keyword.merge(base, opts))
        end
      end

      ### Producer ###
      def producer(topic), do: producer(@client, topic)

      def produce(topic, partition, key, value),
        do: produce(@client, topic, partition, key, value)

      ### Info ###
      def topics, do: topics(@client)
      def agent(agent, topic), do: agent.start_link(@client, topic)
      def offset(topic, time \\ :latest), do: offset(@client, topic, time)
      def leader(topic, partition), do: leader(@client, topic, partition)
      def config, do: with(%{config: config} <- :sys.get_state(__MODULE__), do: config)
    end
  end
end
