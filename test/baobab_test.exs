defmodule BaobabTest do
  use ExUnit.Case
  doctest Baobab

  test "greets the world" do
    assert Baobab.hello() == :world
  end
end
