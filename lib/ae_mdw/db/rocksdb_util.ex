defmodule AeMdw.Db.RocksdbUtil do

  alias AeMdw.Db.Model
  require Model

  import AeMdw.Util
  import AeMdw.Sigil
  import Record

  alias AeMdw.Db.AeUtil

  defdelegate current_height(), to: AeUtil
  defdelegate msecs(msecs), to: AeUtil
  defdelegate date_time(dt), to: AeUtil
  defdelegate prev_block_type(header), to: AeUtil
  defdelegate proto_vsn(height), to: AeUtil
  defdelegate block_hash_to_bi(block_hash), to: AeUtil
  defdelegate status(), to: AeUtil
  defdelegate tx_val(tx_rec, field), to: AeUtil
  defdelegate tx_val(tx_rec, tx_type, field), to: AeUtil
  defdelegate gen_collect(table, init_key_probe, key_tester, progress, new, add, return), to: AeUtil

  @tx_dict_key :"$rocksdb_transaction"

  def read!(tab, key), do: read(tab, key) |> one!

  def read(tab, key) do
    case Process.get(@tx_dict_key) do
      nil ->
        read_d(tab, key)
      tx ->
        read_t(tx, tab, key)
    end
  end

  defp read_d(tab, key) do
    {db, cf} = AeMdw.RocksdbManager.cf_handle!(tab)
    get(db, cf, key)
  end

  defp read_t(tx, tab, key) do
    {_db, cf} = AeMdw.RocksdbManager.cf_handle!(tab)
    case :rocksdb.transaction_get(tx, cf, encode_key(key)) do
      {:ok, val} ->
        [decode_value(val)]
      :not_found ->
        []
    end
  end

  def read_tx!(txi), do: read_tx(txi) |> one!

  def read_tx(txi), do: read(~t[tx], txi)

  def read_block!(bi),
    do: read_block(bi) |> one!

  def read_block(kbi) when is_integer(kbi),
    do: read_block({kbi, -1})

  def read_block({_, _} = bi), do: read(~t[block], bi)

  def block_txi(bi), do: map_one_nil(read_block(bi), &Model.block(&1, :tx_index))

  def next_bi!({_kbi, _mbi} = bi),
    do: {_, _} = next(Model.Block, bi)

  def next_bi!(kbi) when is_integer(kbi),
    do: next_bi!({kbi, -1})

  def do_writes(tab_xs),
    do: do_writes(tab_xs, &write(&1, &2))

  def do_writes(tab_xs, db_write) when is_function(db_write, 2),
    do: Enum.each(tab_xs, fn {tab, xs} -> Enum.each(xs, &db_write.(tab, &1)) end)

  def do_dels(tab_keys),
    do: do_dels(tab_keys, &delete(&1, &2))

  def do_dels(tab_keys, db_delete) when is_function(db_delete, 2),
    do: Enum.each(tab_keys, fn {tab, ks} -> Enum.each(ks, &db_delete.(tab, &1)) end)

  # FIXME: define keypos and schema to simplify this code
  def write_block(block) do
    write(~t[block], block)
  end

  def write(tab, record) when is_record(record) do
    key = elem(record, 1)
    write(tab, key, record)
  end

  defp write(tab, key, val) do
    case Process.get(@tx_dict_key) do
      nil ->
        write_d(tab, key, val)
      tx ->
        write_t(tx, tab, key, val)
    end
  end

  defp write_d(tab, key, val) do
    {db, cf} = AeMdw.RocksdbManager.cf_handle!(tab)
    put(db, cf, key, val)
  end

  defp write_t(tx, tab, key, val) do
    {_db, cf} = AeMdw.RocksdbManager.cf_handle!(tab)
    transaction_put(tx, cf, key, val)
  end

  def delete(tab, key) do
    case Process.get(@tx_dict_key) do
      nil ->
        delete_d(tab, key)
      tx ->
        delete_t(tx, tab, key)
    end
  end

  defp delete_d(tab, key) do
    {db, cf} = AeMdw.RocksdbManager.cf_handle!(tab)
    :rocksdb.delete(db, cf, encode_key(key))
  end

  defp delete_t(tx, tab, key) do
    {_db, cf} = AeMdw.RocksdbManager.cf_handle!(tab)
    :rocksdb.transaction_delete(tx, cf, encode_key(key))
  end

  def first_gen(),
    do: ensure_key!(~t[block], :first) |> (fn {h,-1} -> h end).()

  def last_gen(),
    do: ensure_key!(~t[block], :last) |> (fn {h,-1} -> h end).()

  def first_txi(),
    do: ensure_key!(~t[tx], :first)

  def last_txi(),
    do: ensure_key!(~t[tx], :last)

  def first_time(),
    do: ensure_key!(~t[time], :first) |> (fn {t, _txi} -> t end).()

  def last_time(),
    do: ensure_key!(~t[time], :last) |> (fn {t, _txi} -> t end).()

  def ensure_key!(tab, getter) do
    case do_iter(tab, getter) do
      :"$end_of_table" ->
        raise RuntimeError, message: "can't get #{getter} key for table #{tab}"
      {k, _v} ->
        k
    end
  end

  def collect_keys(tab, acc, start_key, next_fn, progress_fn) do
    case next_fn.(tab, start_key) do
      :"$end_of_table" ->
        acc

      next_key ->
        case progress_fn.(next_key, acc) do
          {:halt, res_acc} -> res_acc
          {:cont, next_acc} -> collect_keys(tab, next_acc, next_key, next_fn, progress_fn)
        end
    end
  end

  def prev(tab, key) do
    {db, cf_handle} = AeMdw.RocksdbManager.cf_handle!(tab)
    {:ok, iter} = :rocksdb.iterator(db, cf_handle, iterate_upper_bound: encode_key(key))
    res = case :rocksdb.iterator_move(iter, :last) do
            {:error, _} ->
              :"$end_of_table"
            {:ok, key, _value} ->
              decode_key(key) # FIXME: return key value pair
          end
    :rocksdb.iterator_close(iter)
    res
  end

  def next(tab, key) do
    {db, cf_handle} = AeMdw.RocksdbManager.cf_handle!(tab)
    {:ok, iter} = :rocksdb.iterator(db, cf_handle, iterate_lower_bound: encode_key(key))
    :rocksdb.iterator_move(iter, :first)
    res = case :rocksdb.iterator_move(iter, :next) do
            {:error, _} ->
              :"$end_of_table"
            {:ok, key, _value} ->
              decode_key(key) # FIXME: return key value pair
          end
    :rocksdb.iterator_close(iter)
    res
  end

  # TODO: maybe rename to first_key?
  def first(tab), do: do_tab_key(tab, :first)
  def last(tab),  do: do_tab_key(tab, :last)
  defp do_tab_key(tab, getter) do
    case do_iter(tab, getter) do
      :"$end_of_table" ->
        :"$end_of_table"
      {k, _} ->
        k
    end
  end

  # FIXME: db connection pool
  defp do_iter(tab, first_or_last) do
    {db, block} = AeMdw.RocksdbManager.cf_handle!(tab)
    {:ok, iter} = :rocksdb.iterator(db, block, [])
    res = case :rocksdb.iterator_move(iter, first_or_last) do
            {:error, _} ->
              :"$end_of_table"
            {:ok, key, value} ->
              {decode_key(key), decode_value(value)}
          end
    :rocksdb.iterator_close(iter)
    res
  end

  def put(db, cf, key, val, opts \\ []) do
    :rocksdb.put(db, cf, encode_key(key), encode_value(val), opts)
  end

  defp transaction_put(tx, cf, key, val) do
    :rocksdb.transaction_put(tx, cf, encode_key(key), encode_value(val))
  end

  def get(db, cf, key, opts \\ []) do
    case :rocksdb.get(db, cf, encode_key(key), opts) do
      {:ok, val} ->
        [decode_value(val)]
      :not_found ->
        []
    end
  end

  def delete(db, cf, key, opts \\ []) do
    :rocksdb.delete(db, cf, encode_key(key), opts)
  end

  def encode_key(key), do: :sext.encode(key)
  def decode_key(bin), do: :sext.decode(bin)
  def encode_value(val), do: :erlang.term_to_binary(val)
  def decode_value(bin), do: :erlang.binary_to_term(bin)

  def select_expired_names(height) do
    {db, cf} = AeMdw.RocksdbManager.cf_handle!(Model.ActiveNameExpiration)
    {:ok, it} = :rocksdb.iterator(db, cf,
      iterate_lower_bound: encode_key({height-1, -1}),
      iterate_upper_bound: encode_key({height+1, -1}))
    data = iter_take_all(it)
    :rocksdb.iterator_close(it)
    for {{^height, name}, _} <- data, do: name
  end

  def select_expired_auctions(height) do
    {db, cf} = AeMdw.RocksdbManager.cf_handle!(Model.AuctionExpiration)
    {:ok, it} = :rocksdb.iterator(db, cf,
      iterate_lower_bound: encode_key({height-1, ""}),
      iterate_upper_bound: encode_key({height+1, ""}))
    data = iter_take_all(it)
    :rocksdb.iterator_close(it)
    for {{^height, name}, {:expiration, _, timeout}} <- data, do: {name, timeout}
  end

  def select_expired_oracles(height) do
    {db, cf} = AeMdw.RocksdbManager.cf_handle!(Model.ActiveOracleExpiration)
    {:ok, it} = :rocksdb.iterator(db, cf,
      iterate_lower_bound: encode_key({height-1, -1}),
      iterate_upper_bound: encode_key({height+1, -1}))
    data = iter_take_all(it)
    :rocksdb.iterator_close(it)
    for {{^height, pubkey}, _} <- data, do: pubkey
  end

  def select_pointee_keys(pk) do
    {db, cf} = AeMdw.RocksdbManager.cf_handle!(Model.Pointee)
    x = :binary.decode_unsigned(pk, :big)
    x0 = :binary.encode_unsigned(x-1, :big)
    x1 = :binary.encode_unsigned(x+1, :big)
    {:ok, it} = :rocksdb.iterator(db, cf,
      iterate_lower_bound: encode_key({x0, {{0,-1}, 0}, ""}),
      iterate_upper_bound: encode_key({x1, {{0,-1}, 0}, ""}))
    data = iter_take_all(it)
    :rocksdb.iterator_close(it)
    for {{^pk, {bi, txi}, k}, _} <- data, do: {bi, txi, k}
  end

  def iter_take_all(it) do
    case :rocksdb.iterator_move(it, :first) do
      {:ok, key, value} ->
        iter_take_all(it, [{decode_key(key), decode_value(value)}])
      {:error, :invalid_iterator} ->
        []
    end
  end

  def iter_take_all(it, acc) do
    case :rocksdb.iterator_move(it, :next) do
      {:ok, key, value} ->
        iter_take_all(it, [{decode_key(key), decode_value(value)} | acc])
      {:error, :invalid_iterator} ->
        acc
    end
  end

  # FIXME, transaction?
  def all(tab), do: dirty_all(tab)

  def dirty_all(tab) do
    {db, cf} = AeMdw.RocksdbManager.cf_handle!(tab)
    {:ok, it} = :rocksdb.iterator(db, cf, [])
    data = iter_take_all(it)
    :rocksdb.iterator_close(it)
    data
  end

  def transaction(f) do
    try do
      db = AeMdw.RocksdbManager.db_handle!()
      {:ok, tx} = :rocksdb.transaction(db, [])
      # nested transactions not supported
      nil = Process.put(@tx_dict_key, tx)
      result = f.()
      :ok = :rocksdb.transaction_commit(tx)
      {:atomic, result}
    rescue
      error ->
        {:aborted, {error, __STACKTRACE__}}
    after
      # FIXME: how to close transaction?
      Process.delete(@tx_dict_key)
    end
  end

  def dirty_read(tab, key) do
    read(tab, key)
  end

  def dirty_next(tab, key), do: next(tab, key)
  def dirty_prev(tab, key), do: prev(tab, key)
  def dirty_first(tab), do: first(tab)
  def dirty_last(tab), do: last(tab)

  def clear_table(tab) do
  end

end
