defmodule Kvasir.Storage do
  # Stream needs support  for:
  #
  # - from (offset or :latest)
  # - to (offset or :latest)
  # - partition (integer)
  # - key (any)
  # - events (list of events)

  @callback child_spec(name :: atom, opts :: Keyword.t()) :: false | map

  @callback contains?(name :: atom, Kvasir.topic(), Kvasir.Offset.t()) :: :maybe | true | false

  @callback freeze(name :: atom, event :: map) :: :ok | {:error, atom}

  @callback stream(name :: atom, Kvasir.topic()) :: {:ok, Stream.t()} | {:error, atom}
end
