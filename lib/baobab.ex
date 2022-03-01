defmodule Baobab do
  @moduledoc """
  Documentation for `Baobab`.
  """

  @configdir "/Users/matt/baobab"
  @typedoc """
  A tuple referring to a specific log entry

  {author, log_id, seqnum}
  """
  @type log_entry :: {binary, non_neg_integer, pos_integer}
  BaseX.prepare_module(
    "Base62",
    "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
    32
  )

  @spec entry_file(log_entry, atom) :: binary | :error
  def entry_file(log_entry, which \\ :name),
    do: handle_seq_file(log_entry, "entry", which)

  @spec payload_file(log_entry, atom) :: binary | :error
  def payload_file(log_entry, which \\ :name),
    do: handle_seq_file(log_entry, "payload", which)

  def is_valid_entry?(entry) do
    valid_sig?(entry) and valid_payload_hash?(entry) and valid_backlink?(entry) and
      valid_lipmaalink?(entry)
  end

  def valid_sig?(%Baobab.Line{
        sig: sig,
        author: author,
        seqnum: seq,
        log_id: log_id
      }) do
    wsig = entry_file({author, log_id, seq}, :content)
    Ed25519.valid_signature?(sig, :binary.part(wsig, {0, byte_size(wsig) - 64}), author)
  end

  def valid_payload_hash?(%Baobab.Line{payload: payload, payload_hash: hash}) do
    YAMFhash.verify(hash, payload) == ""
  end

  def valid_lipmaalink?(%Baobab.Line{seqnum: 1}), do: true

  def valid_lipmaalink?(%Baobab.Line{author: author, log_id: log_id, seqnum: seq, lipmaalink: ll}) do
    case {seq - 1, Lipmaa.linkseq(seq), ll} do
      {n, n, nil} -> true
      {n, n, _} -> false
      {_, n, ll} -> YAMFhash.verify(ll, entry_file({author, log_id, n}, :content)) == ""
    end
  end

  def valid_backlink?(%Baobab.Line{seqnum: 1}), do: true
  def valid_backlink?(%Baobab.Line{backlink: nil}), do: false

  def valid_backlink?(%Baobab.Line{author: author, log_id: log_id, seqnum: seq, backlink: bl}) do
    YAMFhash.verify(bl, entry_file({author, log_id, seq - 1}, :content)) == ""
  end

  def max_entry(author, log_id) do
    a = BaseX.Base62.encode(author)

    max =
      [log_dir(a, Integer.to_string(log_id)), "**", "{entry_*}"]
      |> Path.join()
      |> Path.wildcard()
      |> Enum.map(fn n -> Path.basename(n) end)
      |> Enum.reduce(0, fn n, a ->
        Enum.max([a, n |> String.split("_") |> List.last() |> String.to_integer()])
      end)

    entry_by_log_entry({author, log_id, max})
  end

  def entry_by_log_entry(log_entry) do
    log_entry
    |> Baobab.entry_file(:content)
    |> Baobab.Line.from_binary()
  end

  def key_file(id, which) do
    {:ok, key} =
      Path.join([@configdir, "identity", id, Atom.to_string(which)])
      |> File.read()

    key
  end

  defp handle_seq_file({author, log_id, seq}, name, how) do
    a = BaseX.Base62.encode(author)
    s = Integer.to_string(seq)
    n = Path.join([hashed_dir({a, Integer.to_string(log_id), s}), name <> "_" <> s])

    case how do
      :name ->
        n

      :content ->
        case File.read(n) do
          {:ok, c} -> c
          _ -> :error
        end

      :hash ->
        case File.read(n) do
          {:ok, c} -> YAMFhash.create(c, 0)
          _ -> :error
        end
    end
  end

  defp log_dir(author, log_id) do
    Path.join([@configdir, "content", author, log_id])
  end

  defp hashed_dir({author, log_id, seq}) do
    {top, bot} = seq |> Blake2.hash2b(2) |> Base.encode16(case: :lower) |> String.split_at(2)
    Path.join([log_dir(author, log_id), top, bot])
  end
end
