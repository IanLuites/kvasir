defmodule Kvasir.Metrics.Reporter do
  @moduledoc false
  use GenServer

  @doc false
  @spec child_spec(source :: module, opts :: Keyword.t()) :: :supervisor.child_spec()
  def child_spec(source, opts \\ []) do
    %{
      id: :reporter,
      start: {__MODULE__, :start_link, [source, opts]}
    }
  end

  ## Client API

  @doc false
  @spec start_link(module, opts :: Keyword.t()) :: GenServer.on_start()
  def start_link(source, opts) do
    reporter = Module.concat(source, "Reporter")

    GenServer.start_link(
      __MODULE__,
      %{
        opts: opts,
        source: source
      },
      name: reporter
    )
  end

  @impl GenServer
  def init(%{source: source, opts: opts}) do
    app = to_string(ApplicationX.main())
    {:ok, host} = :inet.gethostname()
    opts = Keyword.put_new(opts, :interval, 5 * 60 * 1_000)

    # Initial Report
    report_application(source, app, host, opts)

    # Start Timer
    Process.send_after(self(), :report, opts[:interval])

    {:ok, %{app: app, source: source, host: host, opts: opts}}
  end

  @impl GenServer
  def handle_info(:report, state = %{app: app, source: source, host: host, opts: opts}) do
    source.metric(["health|", app, "|", host, "|state:healthy"])
    Process.send_after(self(), :report, opts[:interval])

    {:noreply, state}
  end

  @spec report_application(module, atom, String.t() | charlist, Keyword.t()) :: :ok
  defp report_application(source, app, host, opts) do
    main_project = ApplicationX.main_project()

    app_info =
      main_project
      |> Keyword.take(~w(name type version description source_url homepage_url dependencies)a)
      |> Keyword.merge(Keyword.get(opts, :app, []))
      |> Keyword.update(:dependencies, nil, &encode_dependencies/1)
      |> Enum.reject(&(&1 == nil or elem(&1, 1) == nil))
      |> Enum.map(fn {k, v} -> "#{k}:#{v}" end)
      |> Enum.join(",")

    source.metric(["start|", to_string(app), "|", host, "|", app_info])
  end

  @spec encode_dependencies(list) :: String.t()
  defp encode_dependencies(dependencies) do
    dependencies
    |> Map.new(fn {category, deps} ->
      {category,
       Enum.map(deps, fn
         {type, version, opts} -> %{type: type, version: version, opts: Map.new(opts)}
         {type, version} -> %{type: type, version: version, opts: %{}}
       end)}
    end)
    |> Jason.encode!()
    |> Base.encode64()
  end
end
