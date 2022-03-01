defmodule Baobab.Line do
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

  def open(log_entry) do
    log_entry |> Baobab.entry_file(:content) |> from_binary
  end

  def create(author, log_id, payload) do
    %Baobab.Line{seqnum: bl} = Baobab.max_entry(author, log_id) |> IO.inspect()
    seq = bl + 1
    backl = Baobab.entry_file({author, log_id, bl}, :hash)

    ll =
      case Lipmaa.linkseq(seq) do
        ^bl -> nil
        n -> Baobab.entry_file({author, log_id, n}, :hash)
      end

    size = byte_size(payload)
    payload_hash = YAMFhash.create(payload, 0)

    %Baobab.Line{
      tag: <<0>>,
      author: author,
      log_id: log_id,
      seqnum: seq,
      lipmaalink: ll,
      backlink: backl,
      size: size,
      payload_hash: payload_hash,
      payload: payload
    }
  end

  def from_binary(<<tag::binary-size(1), author::binary-size(32), rest::binary>>) do
    add_logid(%Baobab.Line{tag: tag, author: author}, rest)
  end

  defp add_logid(map, bin) do
    {logid, rest} = Varu64.decode(bin)
    add_sequence_num(Map.put(map, :log_id, logid), rest)
  end

  defp add_sequence_num(map, bin) do
    {seqnum, rest} = Varu64.decode(bin)
    add_lipmaa(Map.put(map, :seqnum, seqnum), rest, seqnum)
  end

  # This needs to be extensible sooner or later
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

  defp add_payload(map) do
    validate(
      Map.put(
        map,
        :payload,
        Baobab.payload_file(
          {Map.fetch!(map, :author), Map.fetch!(map, :log_id), Map.fetch!(map, :seqnum)},
          :content
        )
      )
    )
  end

  defp validate(map) do
    case Baobab.is_valid_entry?(map) do
      true -> map
      false -> :error
    end
  end
end
