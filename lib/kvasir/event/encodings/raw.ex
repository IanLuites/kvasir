defmodule Kvasir.Event.Encodings.Raw do
  import Kvasir.Type, only: [do_encode: 3]
  alias Kvasir.Type
  alias Kvasir.Event.{Meta, Registry}

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

  def decode(data, opts \\ []) do
    with {:ok, event} <- find_event(get(data, :type)) do
      do_decode(
        event.__event__(:fields),
        get(data, :payload),
        %{__meta__: Meta.decode(get(data, :meta), opts[:meta])},
        case opts[:process] do
          :create -> &event.create/1
          :struct -> &struct!(event, &1)
          nil -> &struct!(event, &1)
        end
      )
    end
  end

  ### Helpers ###

  defp type(%event{}), do: event.__event__(:type)
  defp meta(%{__meta__: meta}), do: Meta.encode(meta)
  defp payload(event = %type{}), do: do_encode(event, type.__event__(:fields), %{})

  ## Decoding  ##

  defp find_event(event) when is_atom(event) do
    if Code.ensure_compiled?(event),
      do: {:ok, event},
      else: {:error, :unknown_event}
  end

  defp find_event(event) when is_binary(event) do
    if lookup = Registry.lookup(event) do
      find_event(lookup)
    else
      find_event(Module.concat("Elixir", event))
    end
  end

  defp do_decode([], _data, acc, process), do: process.(acc)

  defp do_decode([{property, type, opts} | props], data, acc, process) do
    with {:ok, value} <- fetch(data, property),
         {:ok, parsed_value} <- Type.load(type, value, opts) do
      do_decode(props, data, Map.put(acc, property, parsed_value), process)
    else
      :error ->
        if opts[:optional],
          do: do_decode(props, data, acc, process),
          else: {:error, :missing_field}

      error = {:error, _} ->
        error
    end
  end

  defp fetch(data, field) do
    with :error <- Map.fetch(data, field) do
      Map.fetch(data, to_string(field))
    end
  end

  defp get(data, field) do
    Map.get(data, field) || Map.get(data, to_string(field))
  end
end
