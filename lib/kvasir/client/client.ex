defmodule Kvasir.Client do
  defmacro __using__(opts \\ []) do
    otp = Keyword.fetch!(opts, :otp_app)

    child_spec =
      quote do
        @doc false
        @spec child_spec(Keyword.t()) :: map
        def child_spec(opts \\ []) do
          %{
            id: __MODULE__,
            start: {__MODULE__, :start_link, [opts]},
            type: :supervisor
          }
        end
      end

    # Disabled environments
    if Mix.env() in (opts[:disable] || []) do
      quote do
        @doc false
        @spec start_link(Keyword.t()) :: Supervisor.child_spec()
        def start_link(_opts \\ []) do
          Kvasir.create_registries()

          {:ok, spawn_link(fn -> Process.sleep(:infinity) end)}
        end

        unquote(child_spec)
      end
    else
      quote do
        use GenServer
        import Kvasir.Client.{Consumer, Info}
        alias Kvasir.Client.Producer
        @client Module.concat([__MODULE__, Client])

        @doc false
        @spec start_link(Keyword.t()) :: Supervisor.child_spec()
        def start_link(opts \\ []) do
          unless elem(:application.ensure_all_started(:brod), 0) == :ok do
            raise "Can not start the required dependency: :brod"
          end

          Kvasir.create_registries()

          config = Application.get_env(unquote(otp), __MODULE__, [])

          config =
            case Kvasir.Config.brokers(config[:kafka]) do
              {:ok, brokers} -> Keyword.put(config, :kafka, brokers)
              {:error, reason} -> raise "Invalid broker configuration: #{inspect(reason)}"
            end

          GenServer.start_link(__MODULE__, %{config: config}, name: __MODULE__)
        end

        def init(state) do
          :brod.start_client(state.config[:kafka], @client)

          Enum.each(state.config[:topics] || [], &producer/1)

          spawn_link(fn ->
            Kvasir.Util.AutoStart.start_link(__MODULE__)
            Process.sleep(:infinity)
          end)

          {:ok, state}
        end

        unquote(child_spec)

        ### Consumer ###

        def consume(topic, callback, opts \\ []) do
          with %{config: config} <- :sys.get_state(__MODULE__),
               do: consume(config[:kafka], topic, callback, opts)
        end

        def stream(topic, opts \\ []) do
          with %{config: config} <- :sys.get_state(__MODULE__),
               do: stream(config[:kafka], topic, opts)
        end

        ### Producer ###
        def producer(topic), do: Producer.producer(@client, topic)
        def produce(events), do: Producer.produce(@client, events)

        def produce(topic, partition, key, value),
          do: Producer.produce(@client, topic, partition, key, value)

        ### Info ###
        def topics, do: topics(@client)
        def agent(agent, topic), do: agent.start_link(@client, topic)
        def offset(topic, time \\ :latest), do: offset(@client, topic, time)
        def leader(topic, partition), do: leader(@client, topic, partition)
        def config, do: with(%{config: config} <- :sys.get_state(__MODULE__), do: config)
      end
    end
  end
end
