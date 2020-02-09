defmodule Kvasir.Topic do
  @type t :: %__MODULE__{}
  defstruct ~w(topic key partitions events event_lookup encryption encryption_opts compression compression_opts)a

  defimpl Jason.Encoder, for: __MODULE__ do
    alias Jason.Encoder.Map, as: JMap

    def encode(value, opts) do
      value
      |> Map.from_struct()
      |> Map.delete(:event_lookup)
      |> JMap.encode(opts)
    end
  end
end
