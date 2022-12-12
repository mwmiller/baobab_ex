defmodule Baobab.ClumpMeta do
  alias Baobab.{Identity, Persistence}

  @moduledoc """
  Functions for interacting with clump metadata
  May be useful between consumers to communicate intent
  """

  @max_log :math.pow(2, 64) |> trunc
  @fun_err {:error, "Unresolvable parameters"}

  @doc """
  Create a block of a given type:

  - author: 32 byte-raw or 43-byte base62-encoded value
  - log_id: 64 bit unsigned integer
  - log_spec: `{author, log_id}`

  Returns the current block list
  """
  @spec block(term, binary) :: [term] | {:error, String.t()}
  def block(item, clump_id \\ "default")

  def block(author, clump_id) when is_binary(author) do
    with {:ok, id} <- check_author(author),
         {:ok, cid} <- check_clump_id(clump_id) do
      case blocked?(id, cid) do
        false ->
          Baobab.purge(author, log_id: :all, clump_id: cid)
          do_block(id, cid)

        true ->
          blocks_list(cid)
      end
    else
      err -> err
    end
  end

  def block(log_id, clump_id) when is_integer(log_id) do
    with {:ok, lid} <- check_log_id(log_id),
         {:ok, cid} <- check_clump_id(clump_id) do
      case blocked?(lid, cid) do
        false ->
          Baobab.purge(:all, log_id: lid, clump_id: cid)
          do_block(lid, cid)

        true ->
          blocks_list(cid)
      end
    else
      err -> err
    end
  end

  def block({author, log_id}, clump_id) do
    with {:ok, id} <- check_author(author),
         {:ok, lid} <- check_log_id(log_id),
         {:ok, cid} <- check_clump_id(clump_id) do
      case blocked?({author, log_id}, cid) do
        false ->
          Baobab.purge(id, log_id: lid, clump_id: cid)
          do_block({id, lid}, cid)

        true ->
          blocks_list(cid)
      end
    else
      err -> err
    end
  end

  def block(_, _, _), do: @fun_err

  defp do_block(val, cid) do
    new =
      case Persistence.action(:metadata, cid, :get, :blocks) do
        %MapSet{} = curr -> MapSet.put(curr, val)
        nil -> MapSet.new([val])
      end

    Persistence.action(:metadata, cid, :put, {:blocks, new})
    MapSet.to_list(new)
  end

  defp check_clump_id(cid) do
    case Enum.any?(Baobab.clumps(), fn c -> c == cid end) do
      true -> {:ok, cid}
      false -> {:error, "Unknown clump_id"}
    end
  end

  defp check_author(a) do
    case Identity.as_base62(a) do
      {:error, _} ->
        {:error, "Improper author supplied"}

      id ->
        case Enum.any?(Baobab.Identity.list(), fn {_n, k} -> k == id end) do
          true -> {:error, "May not block identities controlled by Baobab"}
          false -> {:ok, id}
        end
    end
  end

  defp check_log_id(lid) when is_integer(lid) and lid >= 0 and lid <= @max_log, do: {:ok, lid}
  defp check_log_id(_), do: {:error, "Improper log_id"}

  @doc """
  Remove an extant block specified as per `block/2`

  Returns the current block list
  """
  @spec unblock(term, binary) :: :ok | {:error, String.t()}
  def unblock(item, clump_id \\ "default")

  # We can be more liberal saying we'll remove things we never saved.
  def unblock(item, clump_id) do
    with {:ok, cid} <- check_clump_id(clump_id) do
      do_unblock(item, cid)
    else
      err -> err
    end
  end

  defp do_unblock(val, cid) do
    case Persistence.action(:metadata, cid, :get, :blocks) do
      nil ->
        :ok

      %MapSet{} = curr ->
        new = MapSet.delete(curr, val)

        Persistence.action(
          :metadata,
          cid,
          :put,
          {:blocks, new}
        )

        MapSet.to_list(new)
    end
  end

  @doc """
  Returns a boolean indicating whether the supplied spec is blocked on the supplied clump

  Includes the specifications from `block/2` and `{author, log_id, seq_num}`
  """
  @spec blocked?(term) :: boolean | {:error, String.t()}
  def blocked?(item, clump_id \\ "default")

  def blocked?({author, log_id, _seq}, clump_id) do
    with {:ok, cid} <- check_clump_id(clump_id) do
      check_block(get_blocks(cid), author, log_id, 1)
    else
      err -> err
    end
  end

  # We don't check the values closely here because we aren't going to 
  # mutate anything based on them the can give us any nonsense and
  # we can just say it's not in the list
  def blocked?(item, clump_id) do
    with {:ok, cid} <- check_clump_id(clump_id) do
      do_blocked_check(item, cid)
    else
      err -> err
    end
  end

  defp do_blocked_check(val, clump_id), do: clump_id |> get_blocks |> MapSet.member?(val)

  defp get_blocks(cid) do
    case Persistence.action(:metadata, cid, :get, :blocks) do
      nil -> MapSet.new()
      ms -> ms
    end
  end

  @doc """
  Lists current blocks on the supplied clump_id
  """
  @spec blocks_list(binary) :: [term] | {:error, String.t()}
  def blocks_list(clump_id \\ "default") do
    with {:ok, cid} <- check_clump_id(clump_id) do
      cid
      |> get_blocks()
      |> MapSet.to_list()
      |> Enum.map(fn
        {a, l} -> [a, l]
        i -> i
      end)
    else
      err -> err
    end
  end

  @doc """
  Filter out blocked clump logs from a supplied list of entry
  tuples ({`author`, `log_id`, `seq_num`})
  """
  @spec filter_blocked([tuple], binary) :: [tuple] | {:error, String.t()}
  def filter_blocked(entries, clump_id \\ "default") do
    with {:ok, cid} <- check_clump_id(clump_id) do
      block_filter(entries, get_blocks(cid), [])
    else
      err -> err
    end
  end

  defp block_filter([], _, acc), do: Enum.reverse(acc)

  defp block_filter([entry | rest], ms, acc) do
    {a, l, e} =
      case entry do
        [a, l, e] -> {a, l, e}
        {a, l, e} -> {a, l, e}
      end

    case check_block(ms, a, l, e) do
      true -> block_filter(rest, ms, acc)
      false -> block_filter(rest, ms, [entry | acc])
    end
  end

  defp check_block(ms, a, l, _e),
    do: Enum.any?([a, l, {a, l}], fn ls -> MapSet.member?(ms, ls) end)
end
