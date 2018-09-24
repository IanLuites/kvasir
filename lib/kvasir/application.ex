defmodule Kvasir.Application do
  @moduledoc false
  use Application

  def start(_type, _args) do
    create_event_registry()

    children = []

    opts = [strategy: :one_for_one, name: Kvasir.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp create_event_registry do
    events =
      :application.loaded_applications()
      |> Enum.flat_map(fn {app, _, _} ->
        case :application.get_key(app, :modules) do
          :undefined -> []
          {:ok, modules} -> modules
        end
      end)
      |> Enum.filter(&String.starts_with?(to_string(&1), "Elixir.Kvasir.Event.Registry."))

    ensure_unique_events!(events)
    registry = Enum.into(events, %{}, &{&1.type(), &1.module()})

    Code.compiler_options(ignore_module_conflict: true)

    Module.create(
      Kvasir.Event.Registry,
      quote do
        @doc ~S"""
        Lookup events.
        """
        @spec lookup(String.t()) :: module | nil
        def lookup(type), do: Map.get(unquote(Macro.escape(registry)), type)
      end,
      Macro.Env.location(__ENV__)
    )

    Code.compiler_options(ignore_module_conflict: false)
  end

  defp ensure_unique_events!(events) do
    duplicates =
      events
      |> Enum.group_by(& &1.type)
      |> Enum.filter(&(Enum.count(elem(&1, 1)) > 1))

    if duplicates == [] do
      :ok
    else
      error =
        duplicates
        |> Enum.map(fn {k, v} ->
          "  \"#{k}\":\n#{v |> Enum.map(&"    * #{inspect(&1.module)}") |> Enum.join("\n")}"
        end)
        |> Enum.join("\n\n")

      raise "Duplicate Event Types\n\n" <> error
    end
  end
end
