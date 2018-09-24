defmodule Kvasir.Config do
  @default_broker_port 9092
  @kafka_srv_discovery "_kafka._tcp."

  @spec brokers(any) :: {:ok, [{String.t(), integer}]} | {:error, atom}
  def brokers(config)

  def brokers({host, port}) when is_binary(host) and is_integer(port) do
    {:ok, [{host, port}]}
  end

  def brokers(hosts) when is_list(hosts) do
    Enum.reduce_while(hosts, {:ok, []}, fn url, {:ok, acc} ->
      case brokers(url) do
        {:ok, hosts} -> {:cont, {:ok, acc ++ hosts}}
        error -> {:halt, error}
      end
    end)
  end

  def brokers(config) when is_binary(config) do
    config
    |> String.split(",", trim: true)
    |> Enum.map(&String.trim/1)
    |> Enum.map(&URI.parse/1)
    |> Enum.reduce_while({:ok, []}, fn url, {:ok, acc} ->
      case brokers(url) do
        {:ok, hosts} -> {:cont, {:ok, acc ++ hosts}}
        error -> {:halt, error}
      end
    end)
  end

  def brokers(%URI{scheme: "kafka", host: host, port: port}) when host != nil do
    {:ok, [{host, port || @default_broker_port}]}
  end

  def brokers(%URI{scheme: "kafka+srv", host: host}) when host != nil do
    hosts =
      @kafka_srv_discovery
      |> Kernel.<>(host)
      |> String.to_charlist()
      |> :inet_res.lookup(:in, :srv)
      |> Enum.map(fn {_, _, port, host} -> {to_string(host), port} end)

    {:ok, hosts}
  end

  def brokers(_), do: {:error, :invalid_config}
end
