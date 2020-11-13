defmodule Kvasir.Source do
  # Stream needs support  for:
  #
  # - from (offset or :latest)
  # - to (offset or :latest)
  # - partition (integer)
  # - key (any)
  # - events (list of events)
  # - :subscribe

  @callback child_spec(name :: atom, opts :: Keyword.t()) :: false | map

  @callback commit(name :: atom, Kvasir.Topic.t(), Kvasir.Event.t()) ::
              {:ok, Kvasir.Event.t()} | {:error, atom}

  @callback generate_dedicated_publisher(name :: atom, target :: module, Kvasir.Topic.t()) ::
              :ok | {:error, atom}

  @callback contains?(name :: atom, Kvasir.topic(), Kvasir.Offset.t()) :: :maybe | true | false

  @callback subscribe(name :: atom, Kvasir.topic(), opts :: Kvasir.EventSource.stream_opts()) ::
              {:ok, pid} | {:error, atom}
  @callback listen(
              name :: atom,
              Kvasir.topic(),
              callback :: (Kvasir.Event.t() -> :ok | {:error, reason :: atom}),
              opts :: Kvasir.EventSource.stream_opts()
            ) ::
              :ok | {:error, atom}

  @callback stream(name :: atom, Kvasir.topic(), opts :: Kvasir.EventSource.stream_opts()) ::
              {:ok, Stream.t()} | {:error, atom}
end
