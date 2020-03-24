defmodule Mix.Tasks.Events do
  @moduledoc ~S"""
  Manage Kvasir events.
  """
  use Mix.Task
  @preferred_cli_env :dev
  @shortdoc ~S"Manage Kvasir events."
  @strict [major: :boolean, minor: :boolean, patch: :boolean, changelog: :string]
  @impl Mix.Task
  def run(args) do
    events = events()

    case OptionParser.parse(args, strict: @strict) do
      {opts, ["bump", event | _], _} -> bump(events, event, opts)
      {_opts, ["list" | _], _} -> list(events)
      {_opts, [name], _} -> show(events, name)
      _ -> list(events)
    end
  end

  defp columns do
    {columns, 0} = System.cmd("tput", ["cols"])
    String.to_integer(String.trim(columns))
  end

  defp header(header, opts \\ []) do
    bg = Keyword.get(opts, :bg, IO.ANSI.light_yellow_background())
    fg = Keyword.get(opts, :fg, IO.ANSI.black())
    max_width = columns()
    length = String.length(header)

    v =
      case Keyword.get(opts, :align, :center) do
        :center ->
          header
          |> String.pad_trailing(length + div(max_width - length, 2))
          |> String.pad_leading(max_width)

        :left ->
          String.pad_trailing("  " <> header, max_width)

        :right ->
          String.pad_leading(header <> "  ", max_width)
      end

    [bg, fg, v, IO.ANSI.reset(), ?\n]
  end

  defp show(events, event) do
    if e = Enum.find(events, &(inspect(&1) == event || &1.__event__(:type) == event)) do
      source = to_string(e.__info__(:compile)[:source])

      Code.compiler_options(ignore_module_conflict: true)
      Code.compile_file(source)
      Code.compiler_options(ignore_module_conflict: false)

      name = "  " <> inspect(e) <> "  "
      relative = Path.relative_to_cwd(source)
      path = if(String.length(relative) < String.length(source), do: relative, else: source)

      fields =
        Enum.reduce(e.__event__(:fields), [], fn {field, type, opts}, acc ->
          [
            [
              " - #{field}",
              IO.ANSI.italic(),
              " (#{type.name()})",
              IO.ANSI.reset(),
              ?\t,
              List.first(String.split(opts[:doc] || "", "\n")) || "",
              ?\n
            ]
            | acc
          ]
        end)

      history =
        Enum.reduce(e.__event__(:history), [], fn {v, d, doc}, acc ->
          [
            [
              ">> v#{v}",
              IO.ANSI.italic(),
              if(d, do: " (#{d})", else: ""),
              IO.ANSI.reset(),
              ?\n,
              doc,
              ?\n
            ]
            | acc
          ]
        end)

      IO.puts([
        header(name),
        "File:  ",
        IO.ANSI.italic(),
        path,
        IO.ANSI.reset(),
        ?\n,
        "Type:  ",
        IO.ANSI.italic(),
        e.__event__(:type),
        IO.ANSI.reset(),
        ?\n,
        "Error: ",
        IO.ANSI.italic(),
        to_string(e.__event__(:on_error)),
        IO.ANSI.reset(),
        ?\n,
        ?\n,
        e.__event__(:doc),
        ?\n,
        header("Fields", align: :left, bg: IO.ANSI.yellow_background()),
        ?\n,
        :lists.reverse(fields),
        ?\n,
        header("History", align: :left, bg: IO.ANSI.yellow_background()),
        ?\n,
        history
      ])
    else
      IO.puts(:stderr, [IO.ANSI.red(), "Unknown event: #{event}"])
      Sys
    end
  end

  defp bump(events, event, opts) do
    if e = Enum.find(events, &(inspect(&1) == event || &1.__event__(:type) == event)) do
      source = to_string(e.__info__(:compile)[:source])

      Code.compiler_options(ignore_module_conflict: true)
      Code.compile_file(source)
      Code.compiler_options(ignore_module_conflict: false)

      current = e.__event__(:version)
      data = File.read!(source)
      [{split, _} | _] = Regex.run(~r/  event [a-z._]+ do/, data, return: :index)
      {a, b} = String.split_at(data, split)
      now = NaiveDateTime.utc_now()

      changelog =
        opts
        |> Keyword.get(:changelog, "")
        |> String.trim_trailing("\n")
        |> String.split("\n")
        |> Enum.join("\n  ")

      new =
        cond do
          opts[:major] ->
            %{current | major: current.major + 1, minor: 0, patch: 0}

          opts[:minor] ->
            %{current | minor: current.minor + 1, patch: 0}

          opts[:patch] ->
            %{current | patch: current.patch + 1}

          :no_bump ->
            IO.puts(:stderr, [IO.ANSI.red(), "Need to pass `--major`, `--minor`, or `--patch`."])
            System.halt(2)
        end

      [{split, _} | _] =
        Regex.run(Regex.compile!(~s/((upgrade "~> #{current.major - 1}.0")|(end\s*$))/), b,
          return: :index
        )

      {b1, b2} = String.split_at(b, split)

      File.write!(source, [
        a,
        ~s|  @doc ~S"""  \n  #{changelog}\n  """\n  version "#{new}", #{inspect(now)}\n\n|,
        b1,
        ~s/\n  upgrade "~> #{current.major}.0" do\n    {:ok, event}\n  end\n/,
        b2
      ])

      Mix.Task.run("format")
    else
      IO.puts(:stderr, [IO.ANSI.red(), "Unknown event: #{event}"])
      System.halt(1)
    end
  end

  defp list(events) do
    events
    |> Enum.map(
      &[
        inspect(&1),
        to_string(&1.__event__(:version)),
        &1.__event__(:type),
        to_string(&1.__event__(:on_error)),
        :doc |> &1.__event__() |> Kernel.||("") |> String.split("\n") |> List.first()
      ]
    )
    |> Enum.sort_by(&List.first/1)
    |> format_table(["Event", "Version", "Type", "On Error", "Description"])
    |> IO.puts()
  end

  defp format_table(data, columns, dist \\ 2) do
    base = Enum.map(columns, &String.length/1)

    widths =
      Enum.reduce(data, base, fn row, acc -> merge_map(row, acc, &max(String.length(&1), &2)) end)

    width = Enum.sum(widths) + (Enum.count(widths) - 1) * dist

    # {:ok, max_width} = :io.columns()
    # retract = if width > max_width, do: max_width - (width + 3), else: 0

    [
      merge_map(columns, widths, &String.pad_trailing(&1, &2 + dist)),
      ?\n,
      String.duplicate("-", width),
      ?\n,
      data
      |> Enum.map(fn row -> merge_map(row, widths, &String.pad_trailing(&1, &2 + dist)) end)
      |> Enum.intersperse(?\n)
    ]
  end

  defp merge_map(a, b, fun, acc \\ [])
  defp merge_map([], [], _fun, acc), do: :lists.reverse(acc)
  defp merge_map([x | xs], [y | ys], fun, acc), do: merge_map(xs, ys, fun, [fun.(x, y) | acc])

  @spec events :: [module]
  defp events, do: Enum.filter(ApplicationX.modules(), &is_event?/1)

  @spec is_event?(module) :: boolean
  defp is_event?(module) do
    Keyword.has_key?(module.__info__(:attributes), :__event__)
  rescue
    _ -> false
  end
end
