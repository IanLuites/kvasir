defmodule Kvasir.Topic do
  defstruct ~w(topic key partitions events event_lookup)a

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
