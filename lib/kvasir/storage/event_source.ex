defmodule Kvasir.EventSource do
  @type stream_opts :: Keyword.t()

  defmacro __using__(opts \\ []) do
    {event_storage, event_storage_opts} =
      case Macro.expand(
             opts[:source] || raise("Need to set `:source`."),
             __CALLER__
           ) do
        {m, o} -> {m, o}
        m -> {m, []}
      end

    cold_storage = Enum.map(opts[:storage] || [], &Macro.expand(&1, __CALLER__))

    cold_storages =
      cold_storage
      |> Enum.map(&if(is_tuple(&1), do: elem(&1, 0), else: &1))
      |> Enum.with_index()
      |> Enum.map(fn {k, v} -> {k, Module.concat(__CALLER__.module, :"Source#{v + 1}")} end)

    cold_storage_setup =
      cold_storage
      |> Enum.with_index()
      |> Enum.map(fn {cold, index} ->
        {m, o} =
          case cold do
            {m, o} -> {m, o}
            m -> {m, []}
          end

        quote do
          unquote(m).child_spec(
            unquote(Module.concat(__CALLER__.module, :"Source#{index + 1}")),
            Keyword.merge(unquote(o), opts)
          )
        end
      end)

    quote do
      @before_compile unquote(__MODULE__)
      import unquote(__MODULE__), only: [topic: 2, topic: 3]
      Module.register_attribute(__MODULE__, :topics, accumulate: true)

      @doc false
      @spec child_spec(Keyword.t()) :: map
      def child_spec(opts \\ []) do
        %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [opts]}
        }
      end

      @doc false
      @spec start_link(Keyword.t()) ::
              {:ok, pid} | {:error, {:already_started, pid} | {:shutdown, term} | term}
      def start_link(opts \\ []) do
        opts =
          Keyword.put(
            opts,
            :initialize,
            Map.new(__topics__(), fn {k, v} -> {k, v.partitions} end)
          )

        children =
          unquote(cold_storage_setup)
          |> (&[
                __source__().child_spec(
                  Module.concat(__MODULE__, Source),
                  config(:source, Keyword.merge(unquote(event_storage_opts), opts))
                )
                | &1
              ]).()
          |> Enum.reject(&(&1 == false))

        Supervisor.start_link(
          children,
          strategy: :one_for_one,
          name: __MODULE__
        )
      end

      @doc ~S"""
      Publish an event to a given topic.

      ## Examples

      ```elixir
      iex> publish("users", UserEvent.create("bob"))
      :ok
      ```
      """
      @spec publish(String.t(), Kvasir.Event.t(), Keyword.t()) ::
              {:ok, Kvasir.Event.t()} | {:error, atom}
      def publish(topic, event, opts \\ []) do
        with t = %{key: topic_key, partitions: partitions} <-
               __topics__()[topic] || {:error, :unknown_topic} do
          if k = opts[:key] do
            with {:ok, key} <- topic_key.parse(k, opts),
                 {:ok, partition} <- topic_key.partition(key, partitions) do
              e =
                event
                |> Kvasir.Event.set_key(key)
                |> Kvasir.Event.set_partition(partition)
                |> Kvasir.Event.set_topic(topic)

              __source__().commit(
                unquote(Module.concat(__CALLER__.module, Source)),
                t,
                e
              )
            end
          else
            __source__().commit(
              unquote(Module.concat(__CALLER__.module, Source)),
              t,
              event
            )
          end
        end
      end

      @doc ~S"""
      Subscribe to a given topic with a module.

      THe module needs to implement both `init/3` and `event/2`.

      The callbacks are:
       - `init(topic, partition, projection)` (returns `{:ok, state}`)
       - `event(event, state)` (return `:ok` or `{:ok, state}`)

      ## Examples

      ```elixir
      iex> subscribe("users", MyUsersSubscriber)
      :ok
      ```
      """
      @spec subscribe(topic :: String.t(), callback_module :: module, opts :: Keyword.t()) ::
              :ok | {:error, atom}
      def subscribe(topic, callback_module, opts \\ []) do
        if t = __topics__()[topic] do
          unquote(__MODULE__).subscribe(__MODULE__, t, callback_module, opts)
        else
          {:error, :unknown_topic}
        end
      end

      def listen(topic, callback, opts \\ []) do
        if t = __topics__()[topic] do
          unquote(__MODULE__).listen(__MODULE__, t, callback, opts)
        else
          {:error, :unknown_topic}
        end
      end

      @doc ~S"""
      Stream events from a given topic.

      ## Examples

      ```elixir
      iex> stream("users")
      #EventStream<"users">
      ```
      """
      @spec stream(String.t(), Keyword.t()) :: {:ok, EventStream.t()} | {:error, atom}
      def stream(topic, opts \\ []) do
        if t = __topics__()[topic] do
          unquote(__MODULE__).stream(__MODULE__, t, opts)
        else
          {:error, :unknown_topic}
        end
      end

      @doc false
      @spec __source__ :: term
      def __source__, do: unquote(event_storage)

      @doc false
      @spec __storages__ :: term
      def __storages__, do: unquote(cold_storages)

      @doc false
      @spec config(atom, Keyword.t()) :: Keyword.t()
      def config(_name, opts), do: opts
      defoverridable config: 2
    end
  end

  defmacro __before_compile__(_) do
    quote do
      @doc false
      @spec __topics__ :: %{required(String.t()) => Kvasir.Topic.t()}
      def __topics__, do: Map.new(@topics)
    end
  end

  @build_ins %{
    string: Kvasir.Key.String
  }

  defmacro topic(topic, key_format, opts \\ []) do
    setup = %Kvasir.Topic{
      topic: topic,
      key: Macro.expand(@build_ins[key_format] || key_format, __CALLER__),
      partitions: opts[:partitions] || 4,
      events: opts |> Keyword.get(:events, []) |> Enum.map(&Macro.expand(&1, __CALLER__))
    }

    lookup =
      Enum.reduce(
        setup.events,
        {:__block__, [],
         [
           {:@, [context: Elixir, import: Kernel], [{:doc, [context: Elixir], [false]}]},
           {:@, [context: Elixir, import: Kernel],
            [
              {:spec, [context: Elixir],
               [
                 {:"::", [],
                  [
                    {:"#{topic}_event_lookup", [],
                     [
                       {{:., [], [{:__aliases__, [alias: false], [:String]}, :t]}, [], []}
                     ]},
                    {:|, [], [{:module, [], Elixir}, nil]}
                  ]}
               ]}
            ]}
         ]},
        fn event, acc ->
          quote do
            unquote(acc)

            def unquote(:"#{topic}_event_lookup")(unquote(event.__event__(:type))),
              do: unquote(event)
          end
        end
      )

    quote do
      Module.put_attribute(
        __MODULE__,
        :topics,
        {unquote(topic),
         unquote(Macro.escape(setup))
         |> Map.put(
           :event_lookup,
           unquote(
             {:&, [],
              [
                {:/, [context: Elixir, import: Kernel],
                 [
                   {{:., [],
                     [
                       {:__aliases__, [alias: false], [__CALLER__.module]},
                       :"#{topic}_event_lookup"
                     ]}, [], []},
                   1
                 ]}
              ]}
           )
         )
         |> Map.put_new_lazy(:doc, fn ->
           case Module.delete_attribute(__MODULE__, :doc) do
             {_, doc} -> doc
             _ -> ""
           end
         end)}
      )

      unquote(
        Enum.reduce(
          setup.events,
          nil,
          &quote do
            unquote(&2)
            require unquote(&1)
          end
        )
      )

      @doc false
      unquote(lookup)
      def unquote(:"#{topic}_event_lookup")(_), do: nil
    end
  end

  def subscribe(source, topic, callback_module, opts) do
    source.__source__().subscribe(Module.concat(source, Source), topic, callback_module, opts)
  end

  def listen(source, topic, callback, opts) do
    source.__source__().listen(Module.concat(source, Source), topic, callback, opts)
  end

  def stream(source, topic, opts) do
    # raise "Check ColdStorage and EventStorage for criteria."

    if opts[:key] && opts[:partition] do
      raise "Can not set both key and partition, since id determines partition."
    end

    id =
      if k = opts[:key] do
        case topic.key.parse(k, opts) do
          {:ok, kv} -> kv
          {:error, reason} -> raise "Invalid topic key: #{inspect(reason)}"
        end
      end

    events = events(opts[:events])
    missing = Enum.filter(events || [], &(&1 not in topic.events))

    unless missing == [] do
      raise "The following events do not belong to the topic:\n#{
              missing |> Enum.map(&"      #{inspect(&1)}") |> Enum.join("\n")
            }"
    end

    %EventStream{
      source: source,
      topic: topic,
      id: id,
      partition: opts[:partition],
      from: opts[:from],
      events: events,
      endless: opts[:endless] || false
    }
  end

  defp events(nil), do: nil
  defp events(events) when is_list(events), do: events
  defp events(event) when is_atom(event), do: [event]
end
