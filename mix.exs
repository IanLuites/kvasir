defmodule Kvasir.MixProject do
  use Mix.Project
  @version "0.0.3"

  def project do
    [
      app: :kvasir,
      description: "Opinionated Kafka library.",
      version: @version,
      elixir: "~> 1.7",
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package(),

      # Testing
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test
      ],
      # dialyzer: [ignore_warnings: "dialyzer.ignore-warnings", plt_add_deps: true],

      # Docs
      name: "kvasir",
      source_url: "https://github.com/IanLuites/kvasir",
      homepage_url: "https://github.com/IanLuites/kvasir",
      docs: [
        main: "readme",
        extras: ["README.md"]
      ]
    ]
  end

  def package do
    [
      name: :kvasir,
      maintainers: ["Ian Luites"],
      licenses: ["MIT"],
      files: [
        # Elixir
        "lib/kvasir",
        "lib/kvasir.ex",
        ".formatter.exs",
        "mix.exs",
        "README*",
        "LICENSE*"
      ],
      links: %{
        "GitHub" => "https://github.com/IanLuites/kvasir"
      }
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {Kvasir.Application, []}
    ]
  end

  defp deps do
    [
      {:brod, "~> 3.7"},
      {:jason, "~> 1.1"},
      {:ex_doc, ">= 0.0.0", only: :dev}
    ]
  end
end
