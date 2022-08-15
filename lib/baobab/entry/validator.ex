defmodule Baobab.Entry.Validator do
  @moduledoc """
  Validation of `Baobab.Entry` structs
  """
  @doc """
  Validate a `Baobab.Entry` struct

  Includes validation of its available certificate pool
  """
  @spec validate(%Baobab.Entry{}) :: %Baobab.Entry{} | {:error, String.t()}
  def validate(%Baobab.Entry{seqnum: seq, author: author, log_id: log_id} = entry) do
    case validate_entry(entry) do
      :ok ->
        case verify_chain(
               Baobab.certificate_pool(author, seq, log_id),
               {author, log_id},
               :ok
             ) do
          :ok -> entry
          error -> error
        end

      error ->
        error
    end
  end

  def validate(_), do: {:error, "Input is not a Baobab.Entry"}

  defp verify_chain([], _log, answer), do: answer
  defp verify_chain(_links, _log, answer) when is_tuple(answer), do: answer

  defp verify_chain([seq | rest], {author, log_id} = which, _answer) do
    new_answer =
      case Baobab.Entry.retrieve(author, seq, {:entry, log_id, false}) do
        :error ->
          {:error, "Could not retrieve certificate chain seqnum: " <> Integer.to_string(seq)}

        link ->
          validate_link(link)
      end

    verify_chain(rest, which, new_answer)
  end

  defp validate_link(entry) do
    with :ok <- validate_sig(entry),
         :ok <- validate_backlink(entry),
         :ok <- validate_lipmaalink(entry) do
      :ok
    else
      error -> error
    end
  end

  @doc """
  Validate a `Baobab.Entry` without full certificate pool verification.

  Confirms:
    - Signature
    - Payload hash
    - Backlink
    - Lipmaalink
  """
  @spec validate_entry(%Baobab.Entry{}) :: :ok | {:error, String.t()}
  def validate_entry(entry) do
    with :ok <- validate_sig(entry),
         :ok <- validate_payload_hash(entry),
         :ok <- validate_backlink(entry),
         :ok <- validate_lipmaalink(entry) do
      :ok
    else
      error -> error
    end
  end

  @doc """
  Validate the `sig` field of a `Baobab.Entry`
  """
  @spec validate_sig(%Baobab.Entry{}) :: :ok | {:error, String.t()}
  def validate_sig(%Baobab.Entry{
        tag: tag,
        sig: sig,
        author: author,
        seqnum: seq,
        size: size,
        payload_hash: payload_hash,
        log_id: log_id,
        lipmaalink: lipmaa,
        backlink: back
      }) do
    head = tag <> author <> Varu64.encode(log_id) <> Varu64.encode(seq)

    ll =
      case lipmaa do
        nil -> <<>>
        val -> val
      end

    bl =
      case back do
        nil -> <<>>
        val -> val
      end

    tail = Varu64.encode(size) <> payload_hash

    case Ed25519.valid_signature?(sig, head <> ll <> bl <> tail, author) do
      true -> :ok
      false -> {:error, "Invalid signature"}
    end
  end

  @doc """
  Validate the `payload_hash` field of a `Baobab.Entry`
  """
  @spec validate_payload_hash(%Baobab.Entry{}) :: :ok | {:error, String.t()}
  def validate_payload_hash(%Baobab.Entry{payload: payload, payload_hash: hash}) do
    case YAMFhash.verify(hash, payload) do
      <<>> -> :ok
      _ -> {:error, "Invalid payload hash"}
    end
  end

  @doc """
  Validate the `lipmaalink` field of a `Baobab.Entry`
  """
  @spec validate_lipmaalink(%Baobab.Entry{}) :: :ok | {:error, String.t()}
  def validate_lipmaalink(%Baobab.Entry{seqnum: 1, lipmaalink: nil}), do: :ok

  def validate_lipmaalink(%Baobab.Entry{
        author: author,
        log_id: log_id,
        seqnum: seq,
        lipmaalink: ll
      }) do
    case {seq - 1, Lipmaa.linkseq(seq), ll} do
      {n, n, nil} ->
        :ok

      {n, n, _} ->
        {:error, "Invalid lipmaa link when matches backlink"}

      {_, n, ll} ->
        case Baobab.manage_content_store({author, log_id, n}, {:entry, :contents}) do
          :error ->
            {:error, "Missing lipmaalink entry for verificaton"}

          fll ->
            case YAMFhash.verify(ll, fll) do
              <<>> -> :ok
              _ -> {:error, "Invalid lipmaalink hash"}
            end
        end
    end
  end

  @doc """
  Validate the `backlink` field of a `Baobab.Entry`
  """
  @spec validate_backlink(%Baobab.Entry{}) :: :ok | {:error, String.t()}
  def validate_backlink(%Baobab.Entry{seqnum: 1, backlink: nil}), do: :ok
  def validate_backlink(%Baobab.Entry{backlink: nil}), do: {:error, "Missing required backlink"}

  def validate_backlink(%Baobab.Entry{author: author, log_id: log_id, seqnum: seq, backlink: bl}) do
    case Baobab.manage_content_store({author, log_id, seq - 1}, {:entry, :contents}) do
      # We don't have it so we cannot check it.  We'll say it's OK
      # This is required for partial replication to be meaningful.
      # I am sure I will come to regret this post-haste
      :error ->
        :ok

      back_entry ->
        case YAMFhash.verify(bl, back_entry) do
          <<>> -> :ok
          _ -> {:error, "Invalid backlink hash"}
        end
    end
  end
end
