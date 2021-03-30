defmodule AeMdw.Db.RocksdbUtil do

  alias AeMdw.Db.Model
  require Model

  import AeMdw.Util
  import AeMdw.Sigil
  import Record

  def read_block!(bi),
    do: read_block(bi) |> one!

  def read_block(kbi) when is_integer(kbi),
    do: read_block({kbi, -1})

  def read_block({_, _} = bi) do
    {db, cf} = AeMdw.RocksdbManager.cf_handle!(~t[block])
    get(db, cf, bi)
  end

  def next_bi!({_kbi, _mbi} = bi),
    do: {_, _} = next(Model.Block, bi)

  def next_bi!(kbi) when is_integer(kbi),
    do: next_bi!({kbi, -1})

  # FIXME: define keypos and schema to simplify this code
  def write_block(block) do
    {db, cf} = AeMdw.RocksdbManager.cf_handle!(~t[block])
    key = Model.block(block, :index)
    AeMdw.Db.RocksdbUtil.put(db, cf, key, block)
  end

  def write(tab, record) when is_record(record) do
    # FIXME: assuming keypos is 1
    key = elem(record, 1)
    {db, cf} = AeMdw.RocksdbManager.cf_handle!(tab)
    put(db, cf, key, record)
  end

  def first_gen(),
    do: ensure_key!(~t[block], :first) |> (fn {{h,-1},_} -> h end).()

  def last_gen(),
    do: ensure_key!(~t[block], :last) |> (fn {{h,-1},_} -> h end).()

  def ensure_key!(tab, getter) do
    case do_iter(tab, getter) do
      :"$end_of_table" ->
        raise RuntimeError, message: "can't get #{getter} key for table #{tab}"
      k ->
        k
    end
  end

  def first(tab), do: do_iter(tab, :first)
  def last(tab),  do: do_iter(tab, :last)

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

  def get(db, cf, key, opts \\ []) do
    case :rocksdb.get(db, cf, encode_key(key), opts) do
      {:ok, val} ->
        [decode_value(val)]
      :not_found ->
        []
    end
  end

  def encode_key(key), do: :sext.encode(key)
  def decode_key(bin), do: :sext.decode(bin)
  def encode_value(val), do: :erlang.term_to_binary(val)
  def decode_value(bin), do: :erlang.binary_to_term(bin)

end
