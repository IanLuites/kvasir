defmodule Kvasir.Application do
  @moduledoc false
  use Application

  def start(_type, _args) do
    create_event_registry()
    create_auto_start_registry()

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
        @moduledoc ~S"""
        Kvasir Event Registry.
        """

        @doc ~S"""
        Lookup events.
        """
        @spec lookup(String.t()) :: module | nil
        def lookup(type), do: Map.get(events(), type)

        @doc ~S"""
        List all events.
        """
        @spec list :: [module]
        def list, do: Map.values(events())

        @doc ~S"""
        List all events matching the filter.
        """
        @spec list(filter :: Keyword.t()) :: [module]
        def list(filter) do
          events = events()

          events =
            if ns = filter[:namespace] do
              ns = to_string(ns) <> "."
              Enum.filter(events, &String.starts_with?(to_string(elem(&1, 1)), ns))
            else
              events
            end

          events =
            if ns = filter[:type_namespace] do
              ns = ns <> "."
              Enum.filter(events, &String.starts_with?(elem(&1, 0), ns))
            else
              events
            end

          Enum.map(events, &elem(&1, 1))
        end

        @doc ~S"""
        All available events indexed on type.
        """
        @spec events :: map
        def events, do: unquote(Macro.escape(registry))
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

  defp create_auto_start_registry do
    children =
      :application.loaded_applications()
      |> Enum.flat_map(fn {app, _, _} ->
        case :application.get_key(app, :modules) do
          :undefined -> []
          {:ok, modules} -> modules
        end
      end)
      |> Enum.filter(&String.starts_with?(to_string(&1), "Elixir.Kvasir.Util.AutoStart."))

    registry = Enum.group_by(children, & &1.client, & &1.module)

    Code.compiler_options(ignore_module_conflict: true)

    Module.create(
      Kvasir.Util.AutoStart,
      quote do
        @moduledoc ~S"""
        AutoStart registry.
        """

        @doc ~S"""
        Start all auto start modules for a client
        """
        @spec start_link(module) :: term
        def start_link(client) do
          if children = auto_starts()[client] do
            Supervisor.start_link(children,
              strategy: :one_for_one,
              name: Module.concat(client, AutoStartSupervisor)
            )
          end
        end

        @doc ~S"""
        All available auto starts based on client.
        """
        @spec auto_starts :: map
        def auto_starts, do: unquote(Macro.escape(registry))
      end,
      Macro.Env.location(__ENV__)
    )

    Code.compiler_options(ignore_module_conflict: false)
  end
end
