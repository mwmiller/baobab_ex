defmodule BaobabTest do
  use ExUnit.Case
  doctest Baobab

  setup do
    # Maybe we'll actually do some setup someday
    # For now we just want to clear the test spool
    on_exit(fn -> File.rm_rf(Application.fetch_env!(:baobab, :spool_dir) |> Path.expand()) end)
  end

  test "import remote entry" do
    remote_entry = File.read!("test/remote_entry")

    [local_entry | _] = Baobab.import([remote_entry])

    assert %Baobab.Entry{seqnum: 1, log_id: 0, size: 33, tag: <<0>>} = local_entry
    author = local_entry.author

    assert local_entry == Baobab.max_entry(author)
    assert remote_entry == Baobab.max_entry(author, format: :binary)
  end

  test "local use" do
    b62author = Baobab.create_identity("testy")

    root = Baobab.append_log("An entry for testing", "testy")
    assert %Baobab.Entry{seqnum: 1, log_id: 0} = root

    assert %Baobab.Entry{seqnum: 2, log_id: 0} =
             Baobab.append_log("A second entry for testing", "testy")

    other_root = Baobab.append_log("A whole new log!", "testy", log_id: 1)
    assert %Baobab.Entry{seqnum: 1, log_id: 1} = other_root

    assert Baobab.full_log(b62author) |> Enum.count() == 2
    assert Baobab.full_log(b62author, log_id: 1) == [other_root]

    for n <- 3..14 do
      assert %Baobab.Entry{seqnum: ^n, log_id: 0} =
               Baobab.append_log("Entry: " <> Integer.to_string(n), "testy")
    end

    author_key = Baobab.identity_key("testy", :public)
    partial = Baobab.log_at(author_key, 5, format: :binary)
    assert Enum.count(partial) == 8
    latest = Baobab.latest_log(author_key, revalidate: true)
    assert Enum.count(latest) == 4
    full = Baobab.full_log(author_key, log_id: 0)
    assert Enum.count(full) == 14

    assert [^root | _] = Baobab.import(partial)
    assert [^root | _] = latest
    assert [^root | _] = full

    assert Baobab.max_seqnum("testy", log_id: 0) == 14
    assert Baobab.max_seqnum("testy", log_id: 1) == 1
  end
end
