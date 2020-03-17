defmodule Kvasir.Metrics.HealthReporter do
  @moduledoc false
  use GenServer

  @doc false
  @spec child_spec(source :: module, opts :: Keyword.t()) :: :supervisor.child_spec()
  def child_spec(source, opts \\ []) do
    %{
      id: :health_reporter,
      start: {__MODULE__, :start_link, [source, opts]}
    }
  end

  ## Client API

  @doc false
  @spec start_link(module, opts :: Keyword.t()) :: GenServer.on_start()
  def start_link(source, opts) do
    GenServer.start_link(
      __MODULE__,
      %{
        opts: opts,
        source: source
      },
      []
    )
  end

  @impl GenServer
  def init(%{source: source, opts: opts}) do
    app = to_string(ApplicationX.main())
    {:ok, host} = :inet.gethostname()
    opts = Keyword.put_new(opts, :interval, 5 * 60 * 1_000)

    Process.send_after(self(), :report, opts[:interval])

    {:ok, %{app: app, source: source, host: host, opts: opts}}
  end

  @impl GenServer
  def handle_info(:report, state = %{app: app, source: source, host: host, opts: opts}) do
    source.metric(["health|", app, "|", host, "|state:healthy"])
    Process.send_after(self(), :report, opts[:interval])

    {:noreply, state}
  end

  def encode_dependencies(dependencies) do
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
