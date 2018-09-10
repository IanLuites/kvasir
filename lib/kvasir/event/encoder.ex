defmodule Kvasir.Event.Encoder do
  def encode(event) do
    %{
      type: type(event),
      meta: meta(event),
      payload: payload(event)
    }
  end

  defp type(%event{}), do: event.__event__(:type)

  defp meta(%{__meta__: meta}) do
    meta
    |> Map.from_struct()
    |> Enum.reject(&is_nil(elem(&1, 1)))
    |> Enum.into(%{})
  end

  defp payload(event) do
    event
    |> Map.from_struct()
    |> Map.delete(:__meta__)
  end
end
