defmodule AeMdw.Db.RocksdbUtil do

  def first(cf), do: do_iter(cf, :first)
  def last(cf),  do: do_iter(cf, :last)

  # FIXME: db connection pool
  defp do_iter(cf, first_or_last) do
    {db, block} = AeMdw.RocksdbManager.cf_handle!(cf)
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
        {:ok, decode_value(val)}
      :not_found ->
        :not_found
    end
  end

  def encode_key(key), do: :sext.encode(key)
  def decode_key(bin), do: :sext.decode(bin)
  def encode_value(val), do: :erlang.term_to_binary(val)
  def decode_value(bin), do: :erlang.binary_to_term(bin)

end
