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
  def create(payload, clump_id, identity, log_id) do
    author = Baobab.identity_key(identity, :public)
    signer = Baobab.identity_key(identity, :secret)
    prev = Baobab.max_seqnum(author, log_id: log_id, clump_id: clump_id)
    seq = prev + 1
    head = <<0>> <> author <> Varu64.encode(log_id) <> Varu64.encode(seq)

    ll =
      case Lipmaa.linkseq(seq) do
        ^prev -> <<>>
        n -> Baobab.manage_content_store(clump_id, {author, log_id, n}, {:entry, :hash})
      end

    bl =
      case prev do
        0 -> <<>>
        n -> Baobab.manage_content_store(clump_id, {author, log_id, n}, {:entry, :hash})
      end

    tail = Varu64.encode(byte_size(payload)) <> YAMFhash.create(payload, 0)

    meat = head <> ll <> bl <> tail
    sig = Ed25519.signature(meat, signer, author)
    entry = meat <> sig

    Baobab.manage_content_store(
      clump_id,
      {author, log_id, seq},
      {:both, :write, {entry, payload}}
    )

    (entry <> payload) |> from_binary({false, clump_id})
  end

  @doc false
  def store(entry, clump_id, replace)

  def store(
        %Baobab.Entry{
          author: author,
          log_id: log_id,
          seqnum: seq
        } = entry,
        clump_id,
        false
      ) do
    case Baobab.manage_content_store(clump_id, {author, log_id, seq}, {:entry, :exists}) do
      false -> store(entry, clump_id, true)
      true -> entry
    end
  end

  def store(%Baobab.Entry{} = entry, clump_id, true) do
    case Baobab.Entry.Validator.validate(clump_id, entry) do
      %Baobab.Entry{
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
      } ->
        Baobab.manage_content_store(clump_id, {author, log_id, seq}, {:payload, :write, payload})

        contents =
          tag <>
            author <>
            Varu64.encode(log_id) <>
            Varu64.encode(seq) <> option(ll) <> option(bl) <> Varu64.encode(size) <> ph <> sig

        Baobab.manage_content_store(clump_id, {author, log_id, seq}, {:entry, :write, contents})
        entry

      error ->
        error
    end
  end

  def store(_, _, _), do: {:error, "Attempt to store non-Baobab.Entry"}

  defp option(val) when is_nil(val), do: <<>>
  defp option(val), do: val

  @doc false
  def delete(author, seq, log_id, clump_id) do
    entry_id = {author, log_id, seq}
    Baobab.manage_content_store(clump_id, entry_id, {:entry, :delete})
  end

  @doc false
  # Handle the simplest case first
  def retrieve(author, seq, {:binary, log_id, false, clump_id}) do
    entry_id = {author, log_id, seq}

    case Baobab.manage_content_store(clump_id, entry_id, {:both, :contents}) do
      {:error, _} -> :error
      {_, :error} -> :error
      {entry, payload} -> entry <> payload
    end
  end

  # This handles the other three cases:
  # :entry validated or unvalidated
  # :binary validated
  def retrieve(author, seq, {fmt, log_id, validate, clump_id}) do
    entry_id = {author, log_id, seq}
    binary = Baobab.manage_content_store(clump_id, entry_id, {:entry, :contents})

    case {from_binary(binary, validate, clump_id), fmt} do
      {:error, :missing} ->
        :error

      {:error, _} ->
        Baobab.manage_content_store(clump_id, entry_id, {:entry, :delete})
        :error

      {entry, :entry} ->
        entry

      {_, :binary} ->
        binary
    end
  end

  @doc false
  def from_binary(:error, _, _), do: {:error, :missing}
  def from_binary(bin, false, clump_id), do: from_binary(bin, clump_id)

  def from_binary(bin, true, clump_id) do
    case bin |> from_binary(clump_id) do
      %Baobab.Entry{} = entry -> Baobab.Entry.Validator.validate(clump_id, entry)
      _ -> {:error, "Could not create Entry from binary"}
    end
  end

  defp from_binary(bin, _) when byte_size(bin) < 33,
    do: {:error, "Truncated binary cannot be reified"}

  defp from_binary(<<tag::binary-size(1), author::binary-size(32), rest::binary>>, clump_id) do
    add_logid(%Baobab.Entry{tag: tag, author: author}, rest, clump_id)
  end

  defp add_logid(map, bin, clump_id) do
    {logid, rest} = Varu64.decode(bin)
    add_sequence_num(Map.put(map, :log_id, logid), rest, clump_id)
  end

  defp add_sequence_num(map, bin, clump_id) do
    {seqnum, rest} = Varu64.decode(bin)
    add_lipmaa(Map.put(map, :seqnum, seqnum), rest, clump_id)
  end

  defp add_lipmaa(%Baobab.Entry{seqnum: 1} = map, bin, clump_id), do: add_size(map, bin, clump_id)

  defp add_lipmaa(
         %Baobab.Entry{seqnum: seq} = map,
         full = <<yamfh::binary-size(66), rest::binary>>,
         clump_id
       ) do
    ll = Lipmaa.linkseq(seq)

    case ll == seq - 1 do
      true -> add_backlink(map, full, clump_id)
      false -> add_backlink(Map.put(map, :lipmaalink, yamfh), rest, clump_id)
    end
  end

  defp add_backlink(map, <<yamfh::binary-size(66), rest::binary>>, clump_id) do
    add_size(Map.put(map, :backlink, yamfh), rest, clump_id)
  end

  defp add_size(map, bin, clump_id) do
    {size, rest} = Varu64.decode(bin)
    add_payload_hash(Map.put(map, :size, size), rest, clump_id)
  end

  defp add_payload_hash(map, <<yamfh::binary-size(66), rest::binary>>, clump_id) do
    add_sig(Map.put(map, :payload_hash, yamfh), rest, clump_id)
  end

  defp add_sig(map, <<sig::binary-size(64), rest::binary>>, clump_id) do
    add_payload(Map.put(map, :sig, sig), rest, clump_id)
  end

  # If we only got the `entry` portion, assume we might have it on disk
  # The `:error` in the struct can act at a signal that we don't
  defp add_payload(
         %Baobab.Entry{author: author, log_id: log_id, seqnum: seqnum} = map,
         "",
         clump_id
       ) do
    Map.put(
      map,
      :payload,
      Baobab.manage_content_store(clump_id, {author, log_id, seqnum}, {:payload, :contents})
    )
  end

  defp add_payload(map, payload, _) do
    Map.put(map, :payload, payload)
  end
end
