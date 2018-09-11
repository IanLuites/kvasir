defmodule Kvasir.Event.Discovery do
  def discover(nil), do: []

  def discover({:auto, filter}) when is_list(filter) do
    filter = Enum.map(filter, &to_string/1)

    :application.loaded_applications()
    |> Enum.flat_map(fn {app, _, _} ->
      with {:ok, modules} <- :application.get_key(app, :modules) do
        modules
      else
        _ -> []
      end
    end)
    |> Enum.filter(fn m ->
      Enum.any?(filter, &String.starts_with?(to_string(m), &1))
    end)
  end

  def discover({:auto, filter}), do: discover({:auto, [filter]})
  def discover(events), do: events
end
