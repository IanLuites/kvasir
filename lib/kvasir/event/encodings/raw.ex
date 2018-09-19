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

  defp meta(%{__meta__: meta}) do
    meta
    |> Map.from_struct()
    |> Enum.reject(&is_nil(elem(&1, 1)))
    |> Enum.into(%{})
  end

  defp payload(event = %type{}), do: do_encode(event, type.__event__(:fields), %{})
end
