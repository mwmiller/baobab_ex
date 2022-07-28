defmodule Baobab.Entry.Validator do
  @spec validate(Baobab.Entry.t()) :: Baobab.Entry.t() | :error
  def validate(%Baobab.Entry{seqnum: seq, author: author, log_id: log_id} = entry) do
    case valid_entry?(entry) do
      false ->
        :error

      true ->
        case verify_chain(Baobab.certificate_pool({author, log_id, seq}), {author, log_id}, true) do
          false -> :error
          true -> entry
        end
    end
  end

  def validate(_), do: :error

  defp verify_chain([], _log, answer), do: answer
  defp verify_chain(_links, _log, false), do: false

  defp verify_chain([seq | rest], {author, log_id} = which, answer) do
    truth =
      case Baobab.Entry.by_id({author, log_id, seq}, false) do
        :error -> false
        link -> valid_link?(link)
      end

    verify_chain(rest, which, answer and truth)
  end

  defp valid_link?(entry) do
    valid_sig?(entry) and valid_backlink?(entry) and valid_lipmaalink?(entry)
  end

  @spec valid_entry?(Baobab.Entry.t()) :: boolean
  def valid_entry?(entry) do
    valid_sig?(entry) and valid_payload_hash?(entry) and valid_backlink?(entry) and
      valid_lipmaalink?(entry)
  end

  @spec valid_sig?(Baobab.Entry.t()) :: boolean
  def valid_sig?(%Baobab.Entry{
        sig: sig,
        author: author,
        seqnum: seq,
        log_id: log_id
      }) do
    wsig = Baobab.Entry.file({author, log_id, seq}, :content)
    Ed25519.valid_signature?(sig, :binary.part(wsig, {0, byte_size(wsig) - 64}), author)
  end

  @spec valid_payload_hash?(Baobab.Entry.t()) :: boolean
  def valid_payload_hash?(%Baobab.Entry{payload: payload, payload_hash: hash}) do
    YAMFhash.verify(hash, payload) == ""
  end

  @spec valid_lipmaalink?(Baobab.Entry.t()) :: boolean
  def valid_lipmaalink?(%Baobab.Entry{seqnum: 1}), do: true

  def valid_lipmaalink?(%Baobab.Entry{author: author, log_id: log_id, seqnum: seq, lipmaalink: ll}) do
    case {seq - 1, Lipmaa.linkseq(seq), ll} do
      {n, n, nil} -> true
      {n, n, _} -> false
      {_, n, ll} -> YAMFhash.verify(ll, Baobab.Entry.file({author, log_id, n}, :content)) == ""
    end
  end

  @spec valid_backlink?(Baobab.Entry.t()) :: boolean
  def valid_backlink?(%Baobab.Entry{seqnum: 1}), do: true
  def valid_backlink?(%Baobab.Entry{backlink: nil}), do: false

  def valid_backlink?(%Baobab.Entry{author: author, log_id: log_id, seqnum: seq, backlink: bl}) do
    YAMFhash.verify(bl, Baobab.Entry.file({author, log_id, seq - 1}, :content)) == ""
  end
end
