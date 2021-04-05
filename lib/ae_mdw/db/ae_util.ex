defmodule AeMdw.Db.AeUtil do
  alias AeMdw.Db.Model
  require Logger
  require Model

  import AeMdw.{Sigil, Util}
  import AeMdw.Db.RocksdbUtil, except: [current_height: 0,
                                        msecs: 1,
                                        date_time: 1,
                                        prev_block_type: 1,
                                        proto_vsn: 1,
                                        block_hash_to_bi: 1,
                                        status: 0,
                                        tx_val: 2,
                                        tx_val: 3,
                                       ]

  def tx_val(tx_rec, field),
    do: tx_val(tx_rec, elem(tx_rec, 0), field)

  def tx_val(tx_rec, tx_type, field),
    do: elem(tx_rec, Enum.find_index(AeMdw.Node.tx_fields(tx_type), &(&1 == field)) + 1)

  def gen_collect(table, init_key_probe, key_tester, progress, new, add, return) do
    return.(
      case progress.(table, init_key_probe) do
        :"$end_of_table" ->
          new.()

        start_key ->
          case key_tester.(start_key) do
            false ->
              new.()

            other ->
              init_acc =
                case other do
                  :skip -> new.()
                  true -> add.(start_key, new.())
                end

              collect_keys(table, init_acc, start_key, progress, fn key, acc ->
                case key_tester.(key) do
                  false -> {:halt, acc}
                  true -> {:cont, add.(key, acc)}
                  :skip -> {:cont, acc}
                end
              end)
          end
      end
    )
  end

  ##########

  def current_height(),
    do: :aec_blocks.height(ok!(:aec_chain.top_key_block()))

  def msecs(msecs) when is_integer(msecs) and msecs > 0, do: msecs
  def msecs(%Date{} = d), do: msecs(date_time(d))
  def msecs(%DateTime{} = d), do: DateTime.to_unix(d) * 1000

  def date_time(%DateTime{} = dt),
    do: dt

  def date_time(msecs) when is_integer(msecs) and msecs > 0,
    do: DateTime.from_unix(div(msecs, 1000)) |> ok!

  def date_time(%Date{} = d) do
    {:ok, dt, 0} = DateTime.from_iso8601(Date.to_iso8601(d) <> " 00:00:00.0Z")
    dt
  end

  def prev_block_type(header) do
    prev_hash = :aec_headers.prev_hash(header)
    prev_key_hash = :aec_headers.prev_key_hash(header)

    cond do
      :aec_headers.height(header) == 0 -> :key
      prev_hash == prev_key_hash -> :key
      true -> :micro
    end
  end

  def proto_vsn(height) do
    hps = AeMdw.Node.height_proto()
    [{vsn, _} | _] = Enum.drop_while(hps, fn {_vsn, min_h} -> height < min_h end)
    vsn
  end

  def block_txi(bi), do: map_one_nil(read_block(bi), &Model.block(&1, :tx_index))

  def block_hash_to_bi(block_hash) do
    case :aec_chain.get_block(block_hash) do
      {:ok, some_block} ->
        {type, header} =
          case some_block do
            {:key_block, header} -> {:key, header}
            {:mic_block, header, _txs, _fraud} -> {:micro, header}
          end

        height = :aec_headers.height(header)

        case height >= last_gen() do
          true ->
            nil

          false ->
            case type do
              :key ->
                {height, -1}

              :micro ->
                collect_keys(Model.Block, nil, {height, <<>>}, &prev/2, fn
                  {^height, _} = bi, nil ->
                    (Model.block(read_block!(bi), :hash) == block_hash &&
                       {:halt, bi}) || {:cont, nil}

                  _k, nil ->
                    {:halt, nil}
                end)
            end
        end

      :error ->
        nil
    end
  end

  def status() do
    {:ok, top_kb} = :aec_chain.top_key_block()
    {node_syncing?, node_progress} = :aec_sync.sync_progress()
    node_height = :aec_blocks.height(top_kb)
    {mdw_tx_index, mdw_height} = safe_mdw_tx_index_and_height()
    mdw_syncing? = is_pid(Process.whereis(AeMdw.Db.Sync.Supervisor))
    {:ok, version} = :application.get_key(:ae_mdw, :vsn)

    %{
      node_version: :aeu_info.get_version(),
      node_revision: :aeu_info.get_revision(),
      node_height: node_height,
      node_syncing: node_syncing?,
      node_progress: node_progress,
      mdw_version: to_string(version),
      mdw_height: mdw_height,
      mdw_tx_index: mdw_tx_index,
      # MDW is always 1 generation behind
      mdw_synced: node_height == mdw_height + 1,
      mdw_syncing: mdw_syncing?
    }
  end

  defp safe_mdw_tx_index_and_height do
    try do
      mdw_tx_index = last_txi()
      {mdw_height, _} = read_tx!(mdw_tx_index) |> Model.tx(:block_index)
      {mdw_tx_index, mdw_height}
    rescue
      _ ->
        {0, 0}
    end
  end
end
