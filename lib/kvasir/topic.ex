defmodule Kvasir.Topic do
  @type t :: %__MODULE__{}
  defstruct ~w(module topic freeze key partitions events event_lookup encryption encryption_opts compression compression_opts)a

  defimpl Jason.Encoder, for: __MODULE__ do
    alias Jason.Encoder.Map, as: JMap

    def encode(value, opts) do
      value
      |> Map.from_struct()
      |> Map.drop(~w(event_lookup encryption_opts compression_opts)a)
      |> JMap.encode(opts)
    end
  end
end
