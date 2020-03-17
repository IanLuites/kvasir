defmodule Kvasir.Metrics.Sink do
  @moduledoc false
  use GenServer

  def start_link(opts \\ []) do
    {port, opts} = Keyword.pop(opts, :port)
    callback = Keyword.fetch!(opts, :callback)
    GenServer.start_link(__MODULE__, {callback, port || 9125}, opts)
  end

  def init({callback, port}) do
    with {:ok, socket} <- :gen_udp.open(port, [:binary, active: true]) do
      {:ok, {callback, port, socket}}
    end
  end

  def handle_info({:udp, _socket, _address, _port, data}, state = {callback, _, _}) do
    notify(callback, data)

    {:noreply, state}
  end

  defp notify(callback, data) do
    spawn_link(fn ->
      data
      |> String.split("\n")
      |> Enum.each(fn line ->
        case String.split(line, "|", parts: 4) do
          [type, group, id] ->
            callback.(type, group, id, [])

          [type, group, id, opts] ->
            tags =
              opts
              |> String.split(",")
              |> Enum.map(fn tag ->
                case :binary.split(tag, ":") do
                  [a, b] -> {a, b}
                  [a] -> {a, ""}
                end
              end)

            callback.(type, group, id, tags)
        end
      end)
    end)
  end
end
