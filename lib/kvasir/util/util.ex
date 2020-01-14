defmodule Kvasir.Util do
  @moduledoc false

  @doc false
  @spec name(any) :: String.t()
  def name(name) when is_binary(name), do: name
  def name(name) when is_atom(name), do: to_string(name)
  def name({{:., _, names}, _, _}), do: names |> Enum.map(&name/1) |> Enum.join(".")
  def name({name, _, nil}) when is_atom(name), do: to_string(name)

  @doc false
  @spec identity(any) :: any
  def identity(value), do: {:ok, value}

  @doc false
  def documentation(caller) do
    config = Mix.Project.config()
    app = Keyword.get(config, :app)
    version = Keyword.get(config, :version)
    docs = Keyword.get(config, :docs, [])
    package = Keyword.get(config, :package, [])
    dep_path = Keyword.get(config, :deps_path)

    path =
      if String.starts_with?(caller.file, dep_path) do
        dep_depth = dep_path |> Path.split() |> Enum.count()
        caller.file |> Path.split() |> Enum.slice((dep_depth + 1)..-1) |> Path.join()
      else
        "/"
      end

    {hex, hexdocs} =
      case {Keyword.get(package, :name), Keyword.get(package, :organization)} do
        {nil, _} ->
          {nil, nil}

        {app, nil} ->
          {"https://hex.pm/packages/#{app}/#{version}",
           "https://hexdocs.pm/#{app}/#{version}/#{inspect(caller.module)}.html"}

        {app, org} ->
          {"https://hex.pm/packages/#{org}/#{app}/#{version}",
           "https://#{org}.hexdocs.pm/#{app}/#{version}/#{inspect(caller.module)}.html"}
      end

    source =
      case {Keyword.get(docs, :source_url), Keyword.get(docs, :source_ref)} do
        {nil, _} -> nil
        {url, nil} -> url <> "/src/#{path}"
        {url, ref} -> url <> "/src/#{ref}/#{path}"
      end

    {app, version, hex, hexdocs, source}
  end
end
