defmodule KvasirTest do
  use ExUnit.Case
  doctest Kvasir

  test "greets the world" do
    assert Kvasir.hello() == :world
  end
end
