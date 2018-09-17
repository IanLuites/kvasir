defmodule Kvasir.Event.Encodings.JSON do
  alias Kvasir.Event.Encodings.Raw

  def encode(event, opts) do
    with {:ok, data} <- Raw.encode(event, opts) do
      Jason.encode(data, opts)
    end
  end

  def decode(data, opts), do: Jason.decode(data, opts)
end
