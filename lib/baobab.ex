defmodule Baobab do
  @moduledoc """
  Documentation for `Baobab`.
  """

  @configdir "/Users/matt/baobab"

  def entry_file(author, log_id, seq, which \\ :name),
    do: handle_seq_file(author, log_id, seq, "entry", which)

  def payload_file(author, log_id, seq, which \\ :name),
    do: handle_seq_file(author, log_id, seq, "payload", which)

  def key_file(id, which) do
    {:ok, key} =
      Path.join([@configdir, "identity", id, Atom.to_string(which)])
      |> File.read()

    key
  end

  defp handle_seq_file(author, log_id, seq, name, how) do
    a = Base.encode16(author, case: :lower)
    n = Path.join([hashed_dir(a, Integer.to_string(log_id), Integer.to_string(seq)), name])

    case how do
      :name ->
        n

      :content ->
        case File.read(n) do
          {:ok, c} -> c
          any -> any
        end

      :hash ->
        case File.read(n) do
          {:ok, c} -> YAMFhash.create(c, 0)
          any -> any
        end
    end
  end

  defp hashed_dir(author, log_id, seq) do
    {top, bot} = seq |> Blake2.hash2b(2) |> Base.encode16(case: :lower) |> String.split_at(2)
    Path.join([@configdir, "content", author <> "-" <> log_id, top, bot])
  end
end
