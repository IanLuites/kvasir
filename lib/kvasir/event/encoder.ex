defmodule Kvasir.Event.Encoder do
  def encode(event, opts \\ []), do: encoding(opts).encode(event, opts)

  @base_encoders %{
    brod: Kvasir.Event.Encodings.Brod,
    json: Kvasir.Event.Encodings.JSON,
    raw: Kvasir.Event.Encodings.Raw
  }

  @spec encoding(Keyword.t()) :: module
  defp encoding(opts) do
    encoding = opts[:encoding]

    cond do
      is_nil(encoding) -> Kvasir.Event.Encodings.JSON
      :erlang.function_exported(encoding, :encode, 2) -> encoding
      encoding = @base_encoders[encoding] -> encoding
      :no_valid_given -> {:error, :invalid_encoding}
    end
  end
end
