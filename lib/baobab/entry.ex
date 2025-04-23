defmodule Baobab.Entry do
  alias Baobab.Entry.Validator
  alias Baobab.{Identity, Persistence}

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
    author = Identity.key(identity, :public)
    signer = Identity.key(identity, :signing)
    prev = Baobab.max_seqnum(author, log_id: log_id, clump_id: clump_id)
    seq = prev + 1
    head = <<0>> <> author <> Varu64.encode(log_id) <> Varu64.encode(seq)

    ll =
      case Lipmaa.linkseq(seq) do
        ^prev -> <<>>
        n -> Persistence.content(:entry, :hash, {author, log_id, n}, clump_id)
      end

    bl =
      case prev do
        0 -> <<>>
        n -> Persistence.content(:entry, :hash, {author, log_id, n}, clump_id)
      end

    tail = Varu64.encode(byte_size(payload)) <> YAMFhash.create(payload, 0)

    meat = head <> ll <> bl <> tail
    sig = Ed25519.signature(meat, signer, author)
    entry = meat <> sig

    Persistence.content(:both, :write, {author, log_id, seq}, clump_id, {entry, payload})

    {final, ""} = (entry <> payload) |> from_binary({false, clump_id})
    final
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
    case Persistence.content(:entry, :exists, {author, log_id, seq}, clump_id) do
      false -> store(entry, clump_id, true)
      true -> entry
    end
  end

  def store(%Baobab.Entry{author: author, log_id: log_id} = entry, clump_id, true) do
    case Baobab.ClumpMeta.blocked?({author, log_id, 1}, clump_id) do
      true ->
        {:error, "Refusing to store for blocked author"}

      false ->
        case Validator.validate(clump_id, entry) do
          %Baobab.Entry{
            tag: tag,
            log_id: log_id,
            seqnum: seq,
            lipmaalink: ll,
            backlink: bl,
            payload: payload,
            payload_hash: ph,
            sig: sig,
            size: size
          } ->
            contents =
              tag <>
                author <>
                Varu64.encode(log_id) <>
                Varu64.encode(seq) <> option(ll) <> option(bl) <> Varu64.encode(size) <> ph <> sig

            Persistence.content(
              :both,
              :write,
              {author, log_id, seq},
              clump_id,
              {contents, payload}
            )

            entry

          error ->
            error
        end
    end
  end

  def store(_, _, _), do: {:error, "Attempt to store non-Baobab.Entry"}

  defp option(val) when is_nil(val), do: <<>>
  defp option(val), do: val

  @doc false
  def delete(author, seq, log_id, clump_id) do
    entry_id = {author, log_id, seq}
    Persistence.content(:entry, :delete, entry_id, clump_id)
  end

  @doc false
  def from_binaries(stuff, validate, clump_id, acc \\ [])
  def from_binaries(:error, _, _, _), do: [{:error, :missing}]
  def from_binaries("", _, _, acc), do: Enum.reverse(acc)

  def from_binaries(bin, validate, clump_id, acc) do
    case {from_binary(bin, clump_id), validate} do
      {{%Baobab.Entry{} = entry, rest}, true} ->
        Validator.validate(clump_id, entry)
        from_binaries(rest, true, clump_id, [entry | acc])

      {{%Baobab.Entry{} = entry, rest}, false} ->
        from_binaries(rest, true, clump_id, [entry | acc])

      _ ->
        [{:error, "Could not reify fully"}]
    end
  end

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
    {Map.put(
       map,
       :payload,
       Persistence.content(:payload, :contents, {author, log_id, seqnum}, clump_id)
     ), ""}
  end

  defp add_payload(%Baobab.Entry{size: pbytes} = map, full, _) do
    <<payload::binary-size(pbytes), rest::binary>> = full
    {Map.put(map, :payload, payload), rest}
  end
end
