defmodule Kvasir.OffsetTest do
  use ExUnit.Case, async: true
  alias Kvasir.Offset
  import Offset
  doctest Offset

  describe "compare/2" do
    test "eq when exactly the same" do
      a = Offset.create(%{1 => 1, 2 => 2})
      b = Offset.create(%{1 => 1, 2 => 2})

      assert Offset.compare(a, b) == :eq
    end

    test "eq when exactly the same (to map)" do
      a = Offset.create(%{1 => 1, 2 => 2})
      b = %{1 => 1, 2 => 2}

      assert Offset.compare(a, b) == :eq
    end

    test "eq when partition missing" do
      a = Offset.create(%{1 => 1, 2 => 2})
      b = Offset.create(%{2 => 2})

      assert Offset.compare(a, b) == :eq
    end

    test "eq when partition missing (to map)" do
      a = Offset.create(%{1 => 1, 2 => 2})
      b = %{2 => 2}

      assert Offset.compare(a, b) == :eq
    end

    test "lt" do
      a = Offset.create(%{1 => 1, 2 => 1})
      b = Offset.create(%{2 => 2})

      assert Offset.compare(a, b) == :lt
    end

    test "lt (to map)" do
      a = Offset.create(%{1 => 1, 2 => 1})
      b = %{2 => 2}

      assert Offset.compare(a, b) == :lt
    end

    test "gt" do
      a = Offset.create(%{1 => 1, 2 => 3})
      b = Offset.create(%{2 => 2})

      assert Offset.compare(a, b) == :gt
    end

    test "gt (to map)" do
      a = Offset.create(%{1 => 1, 2 => 3})
      b = %{2 => 2}

      assert Offset.compare(a, b) == :gt
    end
  end

  # describe "compare/2 with :earliest" do
  #   test "eq (earliest to earliest)" do
  #     assert Offset.compare(:earliest, :earliest) == :eq
  #   end

  #   test "lt (earliest to any)" do
  #     assert Offset.compare(:earliest, Offset.create(%{0 => 1})) == :lt
  #   end

  #   test "gt (any to earliest)" do
  #     assert Offset.compare(Offset.create(%{0 => 1}), :earliest) == :gt
  #   end
  # end
end
