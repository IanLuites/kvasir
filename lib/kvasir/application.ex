defmodule Kvasir.Application do
  @moduledoc false
  use Application

  def start(_type, _args) do
    Kvasir.create_registries()

    children = []

    opts = [strategy: :one_for_one, name: Kvasir.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
