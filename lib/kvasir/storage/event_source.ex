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

      def child_spec(opts \\ []) do
        %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [opts]}
        }
      end

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
                __event_storage__().child_spec(
                  Module.concat(__MODULE__, Source),
                  Keyword.merge(unquote(event_storage_opts), opts)
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

      def publish(topic, event, opts \\ []) do
        with t = %{key: topic_key, partitions: partitions} <-
               __topics__()[topic] || {:error, :unknown_topic} do
          if k = opts[:key] do
            with {:ok, key} <- topic_key.parse(k, opts) do
              e =
                event
                |> Kvasir.Event.set_key(key)
                |> Kvasir.Event.set_partition(topic_key.partition(key, partitions))
                |> Kvasir.Event.set_topic(topic)

              __event_storage__().commit(
                unquote(Module.concat(__CALLER__.module, Source)),
                t,
                e
              )
            end
          else
            __event_storage__().commit(
              unquote(Module.concat(__CALLER__.module, Source)),
              t,
              event
            )
          end
        end
      end

      def stream(topic, opts \\ []) do
        if t = __topics__()[topic] do
          unquote(__MODULE__).stream(__MODULE__, t, opts)
        else
          {:error, :unknown_topic}
        end
      end

      def __event_storage__, do: unquote(event_storage)
      def __cold_storage__, do: unquote(cold_storages)
    end
  end

  defmacro __before_compile__(_) do
    quote do
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

      unquote(lookup)
      def unquote(:"#{topic}_event_lookup")(_), do: nil

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
    end
  end

  def stream(source, topic, opts) do
    # raise "Check ColdStorage and EventStorage for criteria."

    if opts[:id] && opts[:partition] do
      raise "Can not set both id and partition, since id determines partition."
    end

    %EventStream{
      source: source,
      topic: topic,
      id: opts[:id],
      partition: opts[:partition],
      from: opts[:from]
    }
  end
end
