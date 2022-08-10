defmodule BaobabTest do
  use ExUnit.Case
  doctest Baobab

  setup do
    File.mkdir_p(Application.fetch_env!(:baobab, :spool_dir) |> Path.expand())

    on_exit(fn ->
      File.rm_rf(Application.fetch_env!(:baobab, :spool_dir) |> Path.expand())
    end)
  end

  test "import remote entry" do
    remote_entry = File.read!("test/remote_entry")

    [local_entry | _] = Baobab.import([remote_entry])

    assert %Baobab.Entry{seqnum: 1, log_id: 0, size: 33, tag: <<0>>} = local_entry
    author = local_entry.author

    assert local_entry == Baobab.max_entry(author)
    assert remote_entry == Baobab.max_entry(author, format: :binary)
    assert [{"7nzwZrUYdugEt4WH8FRuWLPekR4MFzrRauIudDhmBmG", 0, 1}] = Baobab.stored_info()
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
    partial = Baobab.log_at(b62author, 5, format: :binary)
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

    assert [{^b62author, 0, 14}, {^b62author, 1, 1}] = Baobab.stored_info()

    assert [
             {:ok, 2},
             {:ok, 3},
             {:ok, 5},
             {:ok, 6},
             {:ok, 7},
             {:ok, 8},
             {:ok, 9},
             {:ok, 10},
             {:ok, 11},
             {:ok, 12}
           ] = Baobab.compact("testy")

    assert [{^b62author, 0, 14}, {^b62author, 1, 1}] = Baobab.stored_info()
  end

  test "errors or not" do
    assert :error = Baobab.identity_key("newb", :secret)
    assert :error = Baobab.identity_key("newb", :public)

    assert [:error] = Baobab.import("")
    assert [:error] = Baobab.import([""])

    assert_raise RuntimeError, fn -> Baobab.log_at("newb", 5) end
    assert [] = Baobab.log_at("0123456789ABCDEF0123456789ABCDEF", 5)
    assert [] = Baobab.log_at("0123456789ABCDEF0123456789ABCDEF0123456789A", 5)
    assert_raise RuntimeError, fn -> Baobab.log_at("0123456789ABCDEF0123456789ABCDEF0123", 5) end
  end
end
