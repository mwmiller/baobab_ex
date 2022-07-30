defmodule Baobab.Entry do
  @moduledoc """
  A struct representing a Baobab entry
  """
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

  @doc false
  def create(payload, identity, log_id) do
    author = Baobab.identity_key(identity, :public)
    signer = Baobab.identity_key(identity, :secret)
    prev = Baobab.max_seqnum(author, log_id: log_id)
    seq = prev + 1
    :ok = handle_seq_file({author, log_id, seq}, "payload", :write, payload)
    head = <<0>> <> author <> Varu64.encode(log_id) <> Varu64.encode(seq)

    ll =
      case Lipmaa.linkseq(seq) do
        ^prev -> <<>>
        n -> file({author, log_id, n}, :hash)
      end

    bl =
      case prev do
        0 -> <<>>
        n -> file({author, log_id, n}, :hash)
      end

    tail = Varu64.encode(byte_size(payload)) <> YAMFhash.create(payload, 0)

    meat = head <> ll <> bl <> tail
    sig = Ed25519.signature(meat, signer, author)
    :ok = handle_seq_file({author, log_id, seq}, "entry", :write, meat <> sig)
    retrieve(author, seq, {:entry, log_id, true})
  end

  @doc false
  def store(%Baobab.Entry{
        tag: tag,
        author: author,
        log_id: log_id,
        seqnum: seq,
        lipmaalink: ll,
        backlink: bl,
        payload: payload,
        payload_hash: ph,
        sig: sig,
        size: size
      }) do
    :ok = handle_seq_file({author, log_id, seq}, "payload", :write, payload)

    contents =
      tag <>
        author <>
        Varu64.encode(log_id) <>
        Varu64.encode(seq) <> option(ll) <> option(bl) <> Varu64.encode(size) <> ph <> sig

    :ok = handle_seq_file({author, log_id, seq}, "entry", :write, contents)

    retrieve(author, seq, {:entry, 0, true})
  end

  defp option(val) when is_nil(val), do: <<>>
  defp option(val), do: val

  @doc false
  # Handle the simplest case first
  def retrieve(author, seq, {:binary, log_id, false}) do
    entry_id = {author, log_id, seq}

    handle_seq_file(entry_id, "entry", :content) <>
      handle_seq_file(entry_id, "payload", :content)
  end

  # This handle the other three cases:
  # :entry validated or unvalidated
  # :binary validated
  def retrieve(author, seq, {fmt, log_id, validate}) do
    entry_id = {author, log_id, seq}

    case {entry_id |> file(:content) |> from_binary(validate), fmt} do
      {:error, _} ->
        handle_seq_file(entry_id, "payload", :delete)
        handle_seq_file(entry_id, "entry", :delete)
        :error

      {entry, :entry} ->
        entry

      {_, :binary} ->
        retrieve(author, seq, {:binary, log_id, false})
    end
  end

  @doc false
  def from_binary(bin, false), do: from_binary(bin)
  def from_binary(bin, true), do: bin |> from_binary |> Baobab.Entry.Validator.validate()
  def from_binary(_, _), do: :error

  defp from_binary(<<tag::binary-size(1), author::binary-size(32), rest::binary>>) do
    add_logid(%Baobab.Entry{tag: tag, author: author}, rest)
  end

  defp from_binary(_), do: :error

  defp add_logid(map, bin) do
    {logid, rest} = Varu64.decode(bin)
    add_sequence_num(Map.put(map, :log_id, logid), rest)
  end

  defp add_sequence_num(map, bin) do
    {seqnum, rest} = Varu64.decode(bin)
    add_lipmaa(Map.put(map, :seqnum, seqnum), rest)
  end

  defp add_lipmaa(%Baobab.Entry{seqnum: 1} = map, bin), do: add_size(map, bin)

  defp add_lipmaa(
         %Baobab.Entry{seqnum: seq} = map,
         full = <<yamfh::binary-size(66), rest::binary>>
       ) do
    ll = Lipmaa.linkseq(seq)

    case ll == seq - 1 do
      true -> add_backlink(map, full)
      false -> add_backlink(Map.put(map, :lipmaalink, yamfh), rest)
    end
  end

  defp add_backlink(map, <<yamfh::binary-size(66), rest::binary>>) do
    add_size(Map.put(map, :backlink, yamfh), rest)
  end

  defp add_size(map, bin) do
    {size, rest} = Varu64.decode(bin)
    add_payload_hash(Map.put(map, :size, size), rest)
  end

  defp add_payload_hash(map, <<yamfh::binary-size(66), rest::binary>>) do
    add_sig(Map.put(map, :payload_hash, yamfh), rest)
  end

  defp add_sig(map, <<sig::binary-size(64), rest::binary>>) do
    add_payload(Map.put(map, :sig, sig), rest)
  end

  # If we onnly got the `entry` portion, assume we might have it on disk
  # THe `:error` in the struct can act at a signal that we don't
  defp add_payload(%Baobab.Entry{author: author, log_id: log_id, seqnum: seqnum} = map, "") do
    Map.put(map, :payload, payload_file({author, log_id, seqnum}, :content))
  end

  defp add_payload(map, payload) do
    Map.put(map, :payload, payload)
  end

  @doc false
  def file(entry_id, which),
    do: handle_seq_file(entry_id, "entry", which)

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

      :delete ->
        File.rm(n)

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
