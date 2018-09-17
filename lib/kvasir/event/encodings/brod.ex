defmodule Kvasir.Event.Encodings.Brod do
  alias Kvasir.Event.Encodings.JSON

  def encode(event, opts) do
    with {:ok, data} <- JSON.encode(event, opts) do
      {:ok, {topic(event), partition(event), key(event), data}}
    end
  end

  def decode(data, opts), do: Jason.decode(data, opts)

  ### Helpers ###

  defp topic(%{__meta__: %{topic: topic}}), do: topic
  defp partition(%{__meta__: %{partition: partition}}), do: partition
  defp key(%{__meta__: %{key: key}}), do: key
end
