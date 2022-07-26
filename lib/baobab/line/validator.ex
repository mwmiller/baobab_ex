defmodule Baobab.Line.Validator do
  @spec validate(Baobab.Line.t()) :: Baobab.Line.t() | :error
  def validate(%Baobab.Line{} = map) do
    case valid_line?(map) do
      true -> map
      false -> :error
    end
  end

  def validate(_), do: :error

  @spec valid_line?(Baobab.Line.t()) :: boolean
  def valid_line?(line) do
    valid_sig?(line) and valid_payload_hash?(line) and valid_backlink?(line) and
      valid_lipmaalink?(line)
  end

  @spec valid_sig?(Baobab.Line.t()) :: boolean
  def valid_sig?(%Baobab.Line{
        sig: sig,
        author: author,
        seqnum: seq,
        log_id: log_id
      }) do
    wsig = Baobab.Line.file({author, log_id, seq}, :content)
    Ed25519.valid_signature?(sig, :binary.part(wsig, {0, byte_size(wsig) - 64}), author)
  end

  @spec valid_payload_hash?(Baobab.Line.t()) :: boolean
  def valid_payload_hash?(%Baobab.Line{payload: payload, payload_hash: hash}) do
    YAMFhash.verify(hash, payload) == ""
  end

  @spec valid_lipmaalink?(Baobab.Line.t()) :: boolean
  def valid_lipmaalink?(%Baobab.Line{seqnum: 1}), do: true

  def valid_lipmaalink?(%Baobab.Line{author: author, log_id: log_id, seqnum: seq, lipmaalink: ll}) do
    case {seq - 1, Lipmaa.linkseq(seq), ll} do
      {n, n, nil} -> true
      {n, n, _} -> false
      {_, n, ll} -> YAMFhash.verify(ll, Baobab.Line.file({author, log_id, n}, :content)) == ""
    end
  end

  @spec valid_backlink?(Baobab.Line.t()) :: boolean
  def valid_backlink?(%Baobab.Line{seqnum: 1}), do: true
  def valid_backlink?(%Baobab.Line{backlink: nil}), do: false

  def valid_backlink?(%Baobab.Line{author: author, log_id: log_id, seqnum: seq, backlink: bl}) do
    YAMFhash.verify(bl, Baobab.Line.file({author, log_id, seq - 1}, :content)) == ""
  end
end
