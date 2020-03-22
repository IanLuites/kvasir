defmodule Kvasir.Metrics.Resolver do
  @moduledoc false
  use GenServer
  require Logger

  @default_port 9125

  @doc false
  @spec refresh(module) :: :refresh
  def refresh(resolver), do: send(resolver, :refresh)

  @doc false
  @spec host(module) :: {tuple, pos_integer}
  def host(resolver), do: GenServer.call(resolver, :lookup)

  @doc false
  @spec listen(module) :: :ok
  def listen(resolver), do: GenServer.call(resolver, {:listener, self()})

  @doc false
  @spec child_spec(opts :: Keyword.t()) :: :supervisor.child_spec()
  def child_spec(opts \\ []) do
    %{
      id: :resolver,
      start: {__MODULE__, :start_link, [opts]}
    }
  end

  ## Client API

  @doc false
  @spec start_link(opts :: Keyword.t()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Module.concat(opts[:source], "Resolver"))
  end

  @impl GenServer
  def init(opts) do
    {host, port} =
      cond do
        url = opts[:url] ->
          uri = URI.parse(url)
          {String.to_charlist(uri.host), uri.port || @default_port}

        host = opts[:host] ->
          {String.to_charlist(host), opts[:port] || @default_port}

        :localhost ->
          {'localhost', @default_port}
      end

    refresh =
      if refresh = opts[:refresh] do
        cond do
          refresh in ~w(ttl infinity)a -> refresh
          refresh in ~w(ttl infinity) -> String.to_existing_atom(refresh)
          :millisecond -> to_integer!(refresh)
        end
      else
        :ttl
      end

    minimum_ttl = to_integer!(Keyword.get(opts, :minimum_ttl, 1_000))

    Logger.debug(fn -> "Kvasir: DNS lookup \"#{host}\"" end)

    case resolve(host, minimum_ttl) do
      {ip, ttl} ->
        refresh_callback(refresh, ttl)

        {:ok,
         %{
           refresh: refresh,
           host: host,
           ip: ip,
           port: port,
           ttl: ttl,
           minimum_ttl: minimum_ttl,
           listeners: []
         }}

      _ ->
        {:error, :could_not_resolve_host}
    end
  end

  @impl GenServer
  def handle_call(:lookup, _, state = %{ip: ip, port: port}) do
    {:reply, {ip, port}, state}
  end

  def handle_call({:listener, pid}, _, state = %{listeners: listeners}) do
    if pid in listeners do
      {:reply, :ok, state}
    else
      {:reply, :ok, %{state | listeners: [pid | listeners]}}
    end
  end

  @impl GenServer
  def handle_info(:refresh, state = %{host: host, minimum_ttl: minimum_ttl}) do
    async_lookup(host, minimum_ttl)

    {:noreply, state}
  end

  def handle_info({:ip, :failed}, state = %{refresh: refresh, ttl: ttl_old}) do
    refresh_callback(refresh, ttl_old)
    {:noreply, state}
  end

  def handle_info(
        {:ip, ip, ttl},
        state = %{ip: ip_old, port: port, refresh: refresh, listeners: listeners}
      ) do
    if ip == ip_old do
      refresh_callback(refresh, ttl)
      {:noreply, state}
    else
      ll = Enum.filter(listeners, &Process.alive?/1)

      Enum.each(ll, &send(&1, {:refresh_header, ip, port}))

      refresh_callback(refresh, ttl)
      {:noreply, %{state | ip: ip, ttl: ttl, listeners: ll}}
    end
  end

  @spec async_lookup(charlist, non_neg_integer) :: pid
  defp async_lookup(host, minimum_ttl) do
    resolver = self()

    spawn_link(fn ->
      Logger.debug(fn -> "Kvasir: DNS lookup \"#{host}\"" end)

      case resolve(host, minimum_ttl) do
        {ip, ttl} -> send(resolver, {:ip, ip, ttl})
        _ -> send(resolver, {:ip, :failed})
      end
    end)
  end

  @spec resolve(charlist, non_neg_integer) :: {tuple, integer} | nil
  defp resolve(host, minimum_ttl) do
    case :inet_res.resolve(host, :in, :a) do
      {:ok, {:dns_rec, _, _, records, _, _}} ->
        find_ip_in_records(records, host, minimum_ttl)

      invalid ->
        Logger.error(fn -> "Kvasir: Resolve failed: #{inspect(invalid)} (\"#{host}\")" end)
        nil
    end
  end

  @spec find_ip_in_records(list, charlist, non_neg_integer) :: {tuple, integer} | nil
  defp find_ip_in_records(records, host, minimum_ttl) do
    case Enum.find_value(records, &match_resolve/1) do
      {ip, ttl} when ttl < minimum_ttl ->
        {ip, minimum_ttl}

      {ip, ttl} ->
        {ip, ttl}

      _ ->
        Logger.error(fn ->
          "Kvasir: Missing resolve record: #{inspect(records)} (\"#{host}\")"
        end)

        nil
    end
  end

  @spec match_resolve(tuple) :: {tuple, integer} | nil
  defp match_resolve({:dns_rr, _, :a, :in, _, ttl, ip, _, _, _}), do: {ip, ttl * 1_000}
  defp match_resolve(_), do: nil

  @spec refresh_callback(:infinity | :ttl | integer, integer) :: :ok
  defp refresh_callback(:infinity, _), do: :ok

  defp refresh_callback(:ttl, ttl) do
    Process.send_after(self(), :refresh, ttl)
    :ok
  end

  defp refresh_callback(refresh, _) when is_integer(refresh) do
    Process.send_after(self(), :refresh, refresh)
    :ok
  end

  @spec to_integer!(String.t() | integer) :: integer | no_return
  defp to_integer!(value)
  defp to_integer!(value) when is_integer(value), do: value
  defp to_integer!(value) when is_binary(value), do: String.to_integer(value)
end
