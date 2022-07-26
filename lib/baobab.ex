defmodule Baobab do
  @moduledoc """
  Documentation for `Baobab`.
  """

  @configdir "/Users/matt/baobab"
  BaseX.prepare_module(
    "Base62",
    "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
    32
  )

  def max_line(author, log_id) do
    a = BaseX.Base62.encode(author)

    max =
      [log_dir(a, Integer.to_string(log_id)), "**", "{entry_*}"]
      |> Path.join()
      |> Path.wildcard()
      |> Enum.map(fn n -> Path.basename(n) end)
      |> Enum.reduce(0, fn n, a ->
        Enum.max([a, n |> String.split("_") |> List.last() |> String.to_integer()])
      end)

    Baobab.Line.by_id({author, log_id, max})
  end

  def key_file(id, which) do
    {:ok, key} =
      Path.join([@configdir, "identity", id, Atom.to_string(which)])
      |> File.read()

    key
  end

  @doc false
  def log_dir(author, log_id) do
    Path.join([@configdir, "content", author, log_id])
  end
end
