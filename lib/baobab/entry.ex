defmodule Baobab.Entry do
  @moduledoc """
  A struct representing a Baobab entry
  """
  @typedoc """
  A tuple referring to a specific log entry

  {author, log_id, seqnum}
  """
  @type entry_id :: {binary, non_neg_integer, pos_integer}

  defstruct tag: <<0>>,
            author:
              <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0>>,
            log_id: 0,
            seqnum: 0,
            lipmaalink: nil,
            backlink: nil,
            size: 0,
            payload_hash:
              <<0, 64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0>>,
            sig:
              <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0>>,
            payload: ""

  @doc """
  Create a new entry from a stored identity

  Does not yet support writing an end of log tag
  """
  def create(payload, identity, log_id \\ 0) do
    author = Baobab.identity_key(identity, :public)
    signer = Baobab.identity_key(identity, :secret)
    %Baobab.Entry{seqnum: bl} = Baobab.max_entry(author, log_id)
    seq = bl + 1
    :ok = handle_seq_file({author, log_id, seq}, "payload", :write, payload)
    head = <<0>> <> author <> Varu64.encode(log_id) <> Varu64.encode(seq)

    ll =
      case Lipmaa.linkseq(seq) do
        ^bl -> <<>>
        n -> file({author, log_id, n}, :hash)
      end

    tail =
      file({author, log_id, bl}, :hash) <>
        Varu64.encode(byte_size(payload)) <> YAMFhash.create(payload, 0)

    meat = head <> ll <> tail
    sig = Ed25519.signature(meat, signer, author)
    :ok = handle_seq_file({author, log_id, seq}, "entry", :write, meat <> sig)
    by_id({author, log_id, seq})
  end

  @doc """
  Retrieve an entry by its id.

  Validated by default, pass `false` for unvalidated retrieval.
  """
  def by_id(entry_id, validate \\ true) do
    entry_id
    |> file(:content)
    |> from_binary(validate)
  end

  defp from_binary(bin, false), do: from_binary(bin)
  defp from_binary(bin, true), do: bin |> from_binary |> Baobab.Entry.Validator.validate()
  defp from_binary(_, _), do: :error

  defp from_binary(<<tag::binary-size(1), author::binary-size(32), rest::binary>>) do
    # This needs better diagnostics eventually
    try do
      add_logid(%Baobab.Entry{tag: tag, author: author}, rest)
    rescue
      _ -> :error
    end
  end

  defp from_binary(_), do: :error

  defp add_logid(map, bin) do
    {logid, rest} = Varu64.decode(bin)
    add_sequence_num(Map.put(map, :log_id, logid), rest)
  end

  defp add_sequence_num(map, bin) do
    {seqnum, rest} = Varu64.decode(bin)
    add_lipmaa(Map.put(map, :seqnum, seqnum), rest, seqnum)
  end

  defp add_lipmaa(map, bin, 1), do: add_size(map, bin)

  defp add_lipmaa(map, full = <<yamfh::binary-size(66), rest::binary>>, seq) do
    ll = Lipmaa.linkseq(seq)

    case ll == seq - 1 do
      true -> add_backlink(map, full, seq)
      false -> add_backlink(Map.put(map, :lipmaalink, yamfh), rest, seq)
    end
  end

  defp add_backlink(map, <<yamfh::binary-size(66), rest::binary>>, _seq) do
    add_size(Map.put(map, :backlink, yamfh), rest)
  end

  defp add_size(map, bin) do
    {size, rest} = Varu64.decode(bin)
    add_payload_hash(Map.put(map, :size, size), rest)
  end

  defp add_payload_hash(map, <<yamfh::binary-size(66), rest::binary>>) do
    add_sig(Map.put(map, :payload_hash, yamfh), rest)
  end

  defp add_sig(map, <<sig::binary-size(64), _::binary>>) do
    add_payload(Map.put(map, :sig, sig))
  end

  defp add_payload(%Baobab.Entry{author: author, log_id: log_id, seqnum: seqnum} = map) do
    Map.put(map, :payload, payload_file({author, log_id, seqnum}, :content))
  end

  @spec file(entry_id, atom) :: binary | :error
  @doc false
  def file(entry_id, which),
    do: handle_seq_file(entry_id, "entry", which)

  @spec payload_file(entry_id, atom) :: binary | :error
  defp payload_file(entry_id, which),
    do: handle_seq_file(entry_id, "payload", which)

  defp handle_seq_file({author, log_id, seq}, name, how, content \\ nil) do
    a = BaseX.Base62.encode(author)
    p = content_dir({a, log_id, seq})
    n = Path.join([p, name <> "_" <> Integer.to_string(seq)])

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

      :write ->
        File.mkdir_p(p)
        File.write(n, content)
    end
  end

  defp content_dir({author, log_id, seq}) when is_integer(log_id),
    do: content_dir({author, Integer.to_string(log_id), seq})

  defp content_dir({author, log_id, seq}) do
    Path.join([Baobab.log_dir(author, log_id), pp(seq, 13), pp(seq, 11)])
  end

  defp pp(n, m), do: n |> rem(m) |> Integer.to_string()
end
