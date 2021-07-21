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
            config(
              unquote(opts[:label] || :"storage#{index + 1}"),
              Keyword.merge(unquote(o), opts)
            )
          )
        end
      end)

    encryption = Macro.expand(opts[:encryption], __CALLER__) || false
    compression = Macro.expand(opts[:compression], __CALLER__) || false

    encryption_opts =
      opt_escape(opts[:encryption_opts], __CALLER__) || {Kvasir.Encryption.AES, []}

    compression_opts =
      opt_escape(opts[:compression_opts], __CALLER__) || {Kvasir.Compression.ZLib, []}

    Module.put_attribute(__CALLER__.module, :encryption, encryption)
    Module.put_attribute(__CALLER__.module, :encryption_opts, encryption_opts)
    Module.put_attribute(__CALLER__.module, :compression, compression)
    Module.put_attribute(__CALLER__.module, :compression_opts, compression_opts)
    Module.put_attribute(__CALLER__.module, :cold_storages, cold_storages)

    quote do
      @after_compile unquote(__MODULE__)
      @before_compile unquote(__MODULE__)
      import unquote(__MODULE__), only: [topic: 2, topic: 3]
      Module.register_attribute(__MODULE__, :topics, accumulate: true)

      @encryption unquote(encryption)
      @encryption_opts unquote(encryption_opts)
      @compression unquote(compression)
      @compression_opts unquote(compression_opts)

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
        topics =
          case opts[:topics] do
            nil -> __topics__()
            filter -> Map.take(__topics__(), filter)
          end

        topics
        |> Enum.map(fn {_, %{module: m}} -> m end)
        |> Enum.each(&apply(&1, :regenerate, []))

        opts =
          Keyword.put(
            opts,
            :initialize,
            Map.new(topics, fn {k, v} -> {k, v.partitions} end)
          )

        source_child_spec =
          __source__().child_spec(
            Module.concat(__MODULE__, Source),
            config(:source, Keyword.merge(unquote(event_storage_opts), opts))
          )

        children =
          if System.get_env("KVASIR_DISABLE_COLD_STORAGE", "false") in ["true", "1"] do
            [source_child_spec]
          else
            [source_child_spec | unquote(cold_storage_setup)]
          end

        ### Testings

        # Code.compile_quoted(
        #   quote do
        #     defmodule unquote(__MODULE__.Metrics.Resolver) do
        #       def host, do: {{127, 0, 0, 1}, 9125}
        #     end
        #   end
        # )

        # children = [Kvasir.Metrics.Dispatcher.child_spec(source: __MODULE__) | children]

        ### Testing

        __topics__()
        |> Map.values()
        |> Enum.each(
          &Kvasir.Event.Encoding.Topic.create(
            &1,
            events: :all,
            overwrite: true,
            extra:
              quote do
                @doc ~S"""
                Generate a topic module made for encoding/decoding
                a subset of events.

                ## Examples

                ```elixir
                iex> MySource.MyTopic.filter([MyEvent])
                MySource.MyTopic.F3227A3894E15B922A187CE92BE2DA902
                ```
                """
                @spec filter([Kvasir.Event.t()]) :: module
                def filter(events) do
                  Kvasir.Event.Encoding.Topic.create(
                    unquote(Macro.escape(&1)),
                    overwrite: false,
                    only: events
                  )
                end
              end
          )
        )

        Supervisor.start_link(
          Enum.reject(children, &(&1 == false)),
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

              commit(t, e)
            end
          else
            commit(t, event)
          end
        end
      end

      defp commit(a, b) do
        __source__().commit(unquote(Module.concat(__CALLER__.module, Source)), a, b)
      end

      # defp commit(a, b) do
      #   with {:ok, e} <-
      #          __source__().commit(unquote(Module.concat(__CALLER__.module, Source)), a, b) do
      #     # metric = [
      #     #   "event|",
      #     #   e.__struct__.__event__(:type),
      #     #   "|",
      #     #   e.__meta__.topic,
      #     #   ":",
      #     #   to_string(e.__meta__.partition),
      #     #   ":",
      #     #   to_string(e.__meta__.offset),
      #     #   "|",
      #     #   "key:#{Kvasir.Event.key(e)}"
      #     # ]

      #     # :poolboy.transaction(
      #     #   __MODULE__.Metrics,
      #     #   &GenServer.cast(&1, {:metric, metric}),
      #     #   5000
      #     # )
      #     # |> IO.inspect()

      #     # IO.puts(["========\n", metric, "\n========"])

      #     {:ok, e}
      #   end
      # end

      @doc ~S"""
      Subscribe to a given topic with a module.

      The callback_module needs to implement both `init/3` and `event/2`.

      The callbacks are:
       - `init(topic, partition, opts)` (returns `{:ok, state}`)
       - `event(event, state)` (return `:ok` or `{:ok, state}`)

      ## Examples

      ```elixir
      iex> subscribe("users", MyUsersSubscriber)
      :ok
      ```
      """
      @spec subscribe(topic :: String.t(), callback_module :: module, opts :: Keyword.t()) ::
              {:ok, pid} | {:error, atom}
      def subscribe(topic, callback_module, opts \\ []) do
        if t = __topics__()[topic] do
          unquote(__MODULE__).subscribe(__MODULE__, t, callback_module, opts)
        else
          {:error, :unknown_topic}
        end
      end

      @doc ~S"""
      Start listening for new events for a given topic.

      On each incoming event the given `callback` is called
      with as only input the event.

      The callback must return `:ok` to continue to the next event.
      All other results will stop the listener.

      ## Examples

      ```elixir
      iex> listen("users", fn event -> IO.inspect(event); :ok end)
      {:ok, <pid>}
      """
      @spec listen(topic :: String.t(), callback :: fun, opts :: Keyword.t()) ::
              {:ok, pid} | {:error, atom}
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

      @doc ~S"""
      Create a test stream of given events from a given topic.

      ## Examples

      ```elixir
      iex> test_stream("users", [])
      []
      ```
      """
      @spec test_stream(String.t(), [Kvasir.Event.t()], Keyword.t()) ::
              {:ok, Enumerable.t()} | {:error, atom}
      def test_stream(topic, events, opts \\ []) do
        if t = __topics__()[topic] do
          unquote(__MODULE__).test_stream(__MODULE__, t, events, opts)
        else
          {:error, :unknown_topic}
        end
      end

      @doc ~S"""
      Generate a dedicated publisher module for a given topic.

      This publisher needs to be started with `start_link`,
      but can also added a child to a Supervisor.

      ## Examples

      ```elixir
      # iex> generate_dedicated_publisher(MyPublisher, "users")
      # iex> MyPublisher.publish(<event>)
      ```
      """
      @spec generate_dedicated_publisher(name :: module, Kvasir.topic(), opts :: Keyword.t()) ::
              :ok | {:error, atom}
      def generate_dedicated_publisher(name, topic, opts \\ []) do
        with t = %{key: topic_key, partitions: partitions} <-
               __topics__()[topic] || {:error, :unknown_topic} do
          __source__().generate_dedicated_publisher(
            unquote(Module.concat(__CALLER__.module, Source)),
            name,
            t,
            opts
          )
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

  def __after_compile__(env, _bytecode) do
    freezers =
      env.module.__topics__
      |> Map.values()
      |> Enum.filter(& &1.freeze)
      |> Enum.map(fn %{module: m} -> Module.concat(m, "Freezer") end)

    Code.compile_quoted(
      quote do
        defmodule unquote(Module.concat(env.module, "Freezer")) do
          @moduledoc ~S"""
          Copy events to cold storage.
          """

          @doc false
          @spec child_spec(Keyword.t()) :: map
          def child_spec(_opts \\ []) do
            %{id: __MODULE__, start: {__MODULE__, :start_link, []}}
          end

          @doc false
          @spec start_link :: {:ok, pid}
          def start_link, do: Supervisor.start_link(unquote(freezers), strategy: :one_for_one)
        end
      end
    )
  end

  @build_ins %{
    string: Kvasir.Key.String
  }

  defp opt_escape(nil, _env), do: nil

  defp opt_escape(opt, env) do
    case Macro.expand(opt, env) do
      {a, b} -> {Macro.expand(a, env), Macro.expand(b, env)}
      a -> {Macro.expand(a, env), []}
    end
  end

  defmacro topic(topic, key_format, opts \\ []) do
    setup = %Kvasir.Topic{
      module:
        topic
        |> String.split(".")
        |> Enum.map(&Macro.camelize/1)
        |> Enum.join(".")
        |> (&Module.concat(__CALLER__.module, &1)).(),
      freeze: Keyword.get(opts, :freeze, true),
      topic: topic,
      key: Macro.expand(@build_ins[key_format] || key_format, __CALLER__),
      partitions: opts[:partitions] || 4,
      events: opts |> Keyword.get(:events, []) |> Enum.map(&Macro.expand(&1, __CALLER__)),
      encryption:
        Macro.expand(opts[:encryption], __CALLER__) ||
          Module.get_attribute(__CALLER__.module, :encryption),
      encryption_opts:
        opt_escape(opts[:encryption_opts], __CALLER__) ||
          Module.get_attribute(__CALLER__.module, :encryption_opts),
      compression:
        Macro.expand(opts[:compression], __CALLER__) ||
          Module.get_attribute(__CALLER__.module, :compression),
      compression_opts:
        opt_escape(opts[:compression_opts], __CALLER__) ||
          Module.get_attribute(__CALLER__.module, :compression_opts)
    }

    cold_topics =
      __CALLER__.module
      |> Module.get_attribute(:cold_storages)
      |> Enum.map(fn {_, cold} ->
        Module.concat([
          setup.module,
          "Freezer",
          String.trim_leading(inspect(cold), inspect(__CALLER__.module) <> ".")
        ])
      end)

    freezers =
      if setup.freeze do
        __CALLER__.module
        |> Module.get_attribute(:cold_storages)
        |> Enum.reduce(
          quote do
            defmodule unquote(Module.concat(setup.module, "Freezer")) do
              @moduledoc ~S"""
              Copy events to cold storage.
              """

              @doc false
              @spec child_spec(Keyword.t()) :: map
              def child_spec(_opts \\ []) do
                %{id: __MODULE__, start: {__MODULE__, :start_link, []}}
              end

              @doc false
              @spec start_link :: {:ok, pid}
              def start_link,
                do: Supervisor.start_link(unquote(cold_topics), strategy: :one_for_one)
            end
          end,
          fn s = {_, cold}, acc ->
            mod =
              Module.concat([
                setup.module,
                "Freezer",
                String.trim_leading(inspect(cold), inspect(__CALLER__.module) <> ".")
              ])

            quote do
              unquote(acc)

              defmodule unquote(mod) do
                @moduledoc false
                use Kvasir.Storage.Freezer,
                  source: unquote(__CALLER__.module),
                  topic: unquote(topic),
                  storage: unquote(s)
              end
            end
          end
        )
      end

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
          t = event.__event__(:type)
          e = event.__event__(:replaced_by) || event

          quote do
            unquote(acc)
            def unquote(:"#{topic}_event_lookup")(unquote(t)), do: unquote(e)
          end
        end
      )

    filter =
      quote do
        @doc ~S"""
        Generate a topic module made for encoding/decoding
        a subset of events.

        ## Examples

        ```elixir
        iex> MySource.MyTopic.filter([MyEvent])
        MySource.MyTopic.F3227A3894E15B922A187CE92BE2DA902
        ```
        """
        @spec filter([Kvasir.Event.t()]) :: module
        def filter(events) do
          Kvasir.Event.Encoding.Topic.create(
            unquote(Macro.escape(setup)),
            true,
            overwrite: false,
            only: events
          )
        end
      end

    regenerate_filter =
      quote do
        unquote(filter)

        @doc false
        def regenerate
        def regenerate, do: :ok
      end

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

      unquote(
        Kvasir.Event.Encoding.Topic.generate(
          setup,
          false,
          quote do
            unquote(filter)

            @doc false
            def regenerate do
              Kvasir.Event.Encoding.Topic.create(
                unquote(Macro.escape(setup)),
                true,
                extra: unquote(Macro.escape(regenerate_filter)),
                overwrite: true,
                events: :all
              )
            end
          end
        )
      )

      unquote(freezers)
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

    partition =
      cond do
        p = opts[:partition] -> p
        id -> id |> topic.key.partition(topic.partitions) |> elem(1)
        :all -> nil
      end

    events = events(opts[:events])
    missing = Enum.filter(events || [], &(&1 not in topic.events))

    unless missing == [] do
      raise "The following events do not belong to the topic:\n#{missing |> Enum.map(&"      #{inspect(&1)}") |> Enum.join("\n")}"
    end

    from =
      if f = opts[:from] do
        cond do
          Kvasir.Offset.empty?(f) ->
            Kvasir.Offset.create(partition, 0)

          is_nil(partition) ->
            f

          o = f.partitions[partition] ->
            %{f | partitions: %{partition => o}}

          :not_set ->
            raise "Partition or Key set, but given `:from` offset does not contain this partition."
        end
      else
        if is_nil(partition) do
          Kvasir.Offset.create(Map.new(0..(topic.partitions - 1), &{&1, 0}))
        else
          Kvasir.Offset.create(partition, 0)
        end
      end

    %EventStream{
      source: source,
      topic: topic,
      id: id,
      partition: partition,
      from: from,
      events: events,
      endless: opts[:endless] || false
    }
  end

  def test_stream(source, topic, events, opts) do
    import Kvasir.Event,
      only: [
        key: 1,
        partition: 1,
        set_key: 2,
        set_key_type: 2,
        set_offset: 2,
        set_partition: 2,
        set_source: 2,
        set_timestamp: 2,
        set_topic: 2,
        type: 1
      ]

    key = if k = opts[:key], do: topic.key.parse!(k)
    partition = opts[:partition]

    p = fn
      nil -> Enum.random(0..(topic.partitions - 1))
      k -> topic.key.partition!(k, topic.partitions)
    end

    event_option = events(opts[:events])
    missing = Enum.filter(event_option || [], &(&1 not in topic.events))

    unless missing == [] do
      raise "The following events do not belong to the topic:\n#{missing |> Enum.map(&"      #{inspect(&1)}") |> Enum.join("\n")}"
    end

    event_filter = if event_option, do: Enum.map(event_option, &type/1)

    {:ok,
     events
     |> Enum.reduce({%{}, []}, fn event, {off, acc} ->
       {e, k} =
         case event do
           {e, k} -> {e, if(k, do: topic.key.parse!(k))}
           e -> {e, key}
         end

       pp = p.(k)
       off = Map.update(off, pp, 0, &(&1 + 1))
       o = off[pp]

       e_set =
         e
         |> set_key(k)
         |> set_key_type(topic.key)
         |> set_offset(o)
         |> set_partition(pp)
         |> set_source(source)
         |> set_timestamp(UTCDateTime.utc_now())
         |> set_topic(topic.topic)

       {off, [e_set | acc]}
     end)
     |> elem(1)
     |> :lists.reverse()
     |> Enum.filter(&(is_nil(event_filter) or type(&1) in event_filter))
     |> Enum.filter(&(is_nil(key) or key(&1) == key))
     |> Enum.filter(&(is_nil(partition) or partition(&1) == partition))}
  end

  defp events(nil), do: nil
  defp events(events) when is_list(events), do: events
  defp events(event) when is_atom(event), do: [event]
end
