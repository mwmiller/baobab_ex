defmodule Baobab do
  @moduledoc """
  Baobab is a pure Elixir implementation of the 
  [Bamboo](https://github.com/AljoschaMeyer/bamboo) append-only log.

  It is fairly opinionated about the filesystem persistence of the logs.
  They are considered to be a spool of the logs as retreived.

  Consumers of this library may wish to place a local copy of the logs in
  a store with better indexing and query properties.

  ### Configuration

  config :baobab, spool_dir: "/tmp"
  """

  BaseX.prepare_module(
    "Base62",
    "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
    32
  )

  @doc """
  Retrieve the latest entry.

  Includes the available certificate pool for its verification.

  Note that the persisted structure is considered verified.  It is not revalidated
  upon retrieval.
  """
  def latest_log(author, log_id \\ 0), do: log_at({author, log_id, max_seqnum(author, log_id)})

  @doc """
  Retrieve the log at a particular `entry_id`.

  Includes the available certificate pool for its verification.

  Note that the persisted structure is considered verified.  It is not revalidated
  upon retrieval.
  """
  def log_at({author, log_id, _seq} = entry_id) do
    entry_id
    |> certificate_pool
    |> Enum.reverse()
    |> Enum.map(fn n -> Baobab.Entry.by_id({author, log_id, n}, false) end)
  end

  @doc """
  Retrieve all available entries in a particular log

  Note that the persisted structure is considered verified.  It is not revalidated
  upon retrieval.
  """
  def full_log(author, log_id \\ 0) do
    gather_all_entries(author, log_id, max_seqnum(author, log_id), [])
  end

  defp gather_all_entries(_, _, 0, acc), do: acc

  defp gather_all_entries(author, log_id, n, acc) do
    newacc =
      case Baobab.Entry.by_id({author, log_id, n}, false) do
        :error -> acc
        entry -> [entry | acc]
      end

    gather_all_entries(author, log_id, n - 1, newacc)
  end

  @doc """
  Compute the current certificate pool path for a given `entry_id` tuple.

  The certificate pool may include entries beyond the given entry in order
  to ensure consistency with the larger structure.
  """
  def certificate_pool({author, log_id, seq}) do
    max = max_seqnum(author, log_id)
    seq |> Lipmaa.cert_pool() |> Enum.reject(fn n -> n > max end)
  end

  @doc """
  Retrieve the latest sequence number on a particular log identified by the
  author key and log number
  """
  def max_seqnum(author, log_id \\ 0) do
    a = BaseX.Base62.encode(author)

    [log_dir(a, log_id), "**", "{entry_*}"]
    |> Path.join()
    |> Path.wildcard()
    |> Enum.map(fn n -> Path.basename(n) end)
    |> Enum.reduce(0, fn n, a ->
      Enum.max([a, n |> String.split("_") |> List.last() |> String.to_integer()])
    end)
  end

  @doc """
  Retrieve the latest entry on a particular log identified by the
  author key and log number
  """
  def max_entry(author, log_id \\ 0) do
    Baobab.Entry.by_id({author, log_id, max_seqnum(author, log_id)})
  end

  @doc """
  Retrieve the key for a stored identity.

  Can be either the `:public` or `:secret` key
  """
  def identity_key(id, which) do
    {:ok, key} =
      Path.join([
        Application.fetch_env!(:baobab, :spool_dir),
        "identity",
        id,
        Atom.to_string(which)
      ])
      |> File.read()

    key
  end

  @doc false
  def log_dir(author, log_id) when is_integer(log_id),
    do: log_dir(author, Integer.to_string(log_id))

  def log_dir(author, log_id) do
    Path.join([Application.fetch_env!(:baobab, :spool_dir), "content", author, log_id])
  end
end
