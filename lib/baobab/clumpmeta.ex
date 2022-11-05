defmodule Baobab.ClumpMeta do
  alias Baobab.{Identity, Persistence}

  @moduledoc """
  Functions for interacting with clump metadata
  May be useful between consumers to communicate intent
  """

  @doc """
  Block the given log author on the supplied clump_id

  Authors should be supplied as 32 byte-raw or 43-byte base62-encoded values.

  May not be applied to member of local `identities`
  """
  @spec block_author(binary, binary) :: :ok | {:error, String.t()}
  def block_author(author, clump_id \\ "default") do
    case Identity.as_base62(author) do
      {:error, _} ->
        {:error, "Improper identity supplied"}

      id ->
        case Enum.any?(Baobab.Identity.list(), fn {_n, k} -> k == id end) do
          true ->
            {:error, "May not block identities controlled by Baobab"}

          false ->
            new =
              case Persistence.action(:metadata, clump_id, :get, :author_blocks) do
                %MapSet{} = curr -> MapSet.put(curr, author)
                nil -> MapSet.new([author])
              end

            Persistence.action(:metadata, clump_id, :put, {:author_blocks, new})
            # We're not going to take new entries, let's drop the old.
            Baobab.purge(author, log_id: :all, clump_id: clump_id)
            :ok
        end
    end
  end

  @doc """
  Unblock the given log author on the supplied clump_id

  Authors should be supplied as 32 byte-raw or 43-byte base6-encoded values.
  """
  @spec unblock_author(binary, binary) :: :ok | {:error, String.t()}
  def unblock_author(author, clump_id \\ "default") do
    case Identity.as_base62(author) do
      {:error, _} ->
        {:error, "Improper identity supplied"}

      id ->
        case Persistence.action(:metadata, clump_id, :get, :author_blocks) do
          nil ->
            :ok

          %MapSet{} = curr ->
            Persistence.action(
              :metadata,
              clump_id,
              :put,
              {:author_blocks, MapSet.delete(curr, id)}
            )

            :ok
        end
    end
  end

  @doc """
  Returns a boolean indicating whether a given author is blocked on the supplied clump
  """
  @spec blocked_author?(binary) :: boolean | {:error, String.t()}
  def blocked_author?(author, clump_id \\ "default") do
    case Identity.as_base62(author) do
      {:error, _} ->
        {:error, "Improper identity supplied"}

      id ->
        case Persistence.action(:metadata, clump_id, :get, :author_blocks) do
          nil -> false
          ms -> MapSet.member?(ms, id)
        end
    end
  end

  @doc """
  Lists currently blocked authors on the supplied clump_id

  Results are returned as the base62-encoded identity
  """
  @spec list_blocked_authors(binary) :: [binary]
  def list_blocked_authors(clump_id \\ "default") do
    case Persistence.action(:metadata, clump_id, :get, :author_blocks) do
      nil -> []
      ms -> MapSet.to_list(ms)
    end
  end

  @doc false
  # Consumers shouldn't need to run this independently, I am willing to be
  # proven wrong on this point.
  def purge_blocked_authors(clump_id \\ "default") do
    clump_id
    |> list_blocked_authors()
    |> Enum.map(fn a -> Baobab.purge(a, log_id: :all, clump_id: clump_id) end)

    :ok
  end
end
