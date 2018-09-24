defmodule Kvasir.Event.Encodings.Raw do
  import Kvasir.Type, only: [do_encode: 3]

  def encode(event, _opts) do
    with {:ok, payload} <- payload(event) do
      {:ok,
       %{
         type: type(event),
         meta: meta(event),
         payload: payload
       }}
    end
  end

  def decode(data, _opts), do: {:ok, data}

  ### Helpers ###

  defp type(%event{}), do: event.__event__(:type)
  defp meta(%{__meta__: meta}), do: Kvasir.Event.Meta.encode(meta)
  defp payload(event = %type{}), do: do_encode(event, type.__event__(:fields), %{})
end
