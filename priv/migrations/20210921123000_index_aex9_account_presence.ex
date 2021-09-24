defmodule AeMdw.Migrations.IndexAex9AccountPresence do
  @moduledoc """
  Indexes missing Aex9AccountPresence based on contract raw logs.
  """
  alias AeMdw.Application, as: MdwApp
  alias AeMdw.Node, as: AE
  alias AeMdw.Node.Db, as: DBN

  alias AeMdw.Contract
  alias AeMdw.Db.Contract, as: DbContract
  alias AeMdw.Db.Format
  alias AeMdw.Db.Model
  alias AeMdw.Db.Origin
  alias AeMdw.Db.Util
  alias AeMdw.Validate
  alias AeMdwWeb.Helpers.Aex9Helper
  alias AeMdwWeb.Views.Aex9ControllerView
  # alias AeMdw.Db.Sync.Transaction, as: SyncTx
  alias AeMdw.Sync.Supervisor, as: SyncSup

  import AeMdw.Util, only: [ok!: 1]
  require Model
  require Ex2ms

  defmodule Account2Fix do
    defstruct [:account_pk, :contract_pk]
  end

  defmodule Aex9CallIndex do
    defstruct [:height, :mbi, :call_txi]
  end

  @doc """

  """
  @spec run() :: {:ok, {pos_integer(), pos_integer()}}
  def run do
    begin = DateTime.utc_now()

    if :ok != Application.ensure_started(:ae_mdw) do
      IO.puts("Ensure sync tables...")
      SyncSup.init_tables()
      MdwApp.init(:contract_cache)
      MdwApp.init(:db_state)
    end

    indexed_count =
      fetch_aex9_contracts()
      |> accounts_without_balance(60, 30)
      |> Enum.map(fn %Account2Fix{
                       contract_pk: contract_pk
                     } = account2fix ->
        # contract_pk
        # |> origin_txi()
        count =
          {:contract, contract_pk}
          |> Origin.tx_index()
          |> fetch_contract_calls_indexes()
          |> Enum.group_by(fn %Aex9CallIndex{height: height} -> height end)
          |> Enum.map(fn {height, calls_idxs} ->
            _count = reindex_account_aex9_mbs_txs(account2fix, height, calls_idxs)
          end)
          |> Enum.sum()

        count
      end)
      |> Enum.sum()

    duration = DateTime.diff(DateTime.utc_now(), begin)
    IO.puts("Indexed #{indexed_count} records in #{duration}s")

    {:ok, {indexed_count, duration}}
  end

  defp fetch_aex9_contracts() do
    aex9_spec =
      Ex2ms.fun do
        {:aex9_contract, :_, :_} = record -> record
      end

    Model.Aex9Contract
    |> Util.select(aex9_spec)
    |> Enum.map(fn Model.aex9_contract(index: {_name, _symbol, txi, _decimals}) ->
      txi
      |> Util.read_tx!()
      |> Format.to_map()
      |> get_in(["tx", "contract_id"])
      |> Validate.id!([:contract_pubkey])
    end)
  end

  defp accounts_without_balance(contract_list, drop, take) do
    last_txi = Util.last_txi()

    contract_list
    |> Enum.drop(drop)
    |> Enum.take(take)
    |> Enum.map(fn contract_pk ->
      {amounts, _last_block_tuple} = DBN.aex9_balances(contract_pk)

      {contract_pk, normalized_amounts(amounts)}
    end)
    |> Enum.flat_map(fn {contract_pk, amounts} ->
      amounts
      |> Map.keys()
      |> Enum.filter(fn account_id -> Map.get(amounts, account_id) > 0 end)
      |> Enum.map(fn account_id ->
        %Account2Fix{
          account_pk: AeMdw.Validate.id!(account_id, [:account_pubkey]),
          contract_pk: contract_pk
        }
      end)
    end)
    |> Enum.filter(fn %Account2Fix{account_pk: account_pk} ->
      account_balances_is_empty?(account_pk, last_txi)
    end)
    |> Enum.map(fn %Account2Fix{account_pk: account_pk, contract_pk: contract_pk} = acc ->
      IO.inspect "###### EMPTY BAL #####"
      IO.puts :aeser_api_encoder.encode(:account_pubkey, account_pk)
      IO.puts :aeser_api_encoder.encode(:contract_pubkey, contract_pk)
      acc
    end)
  end

  # defp origin_txi(contract_pk) do
  #   contract_create_spec =
  #     Ex2ms.fun do
  #       {:origin, {:contract_create_tx, ^contract_pk, create_txi}, :_} -> create_txi
  #     end

  #   Model.Origin
  #   |> Util.select(contract_create_spec)
  #   |> List.first()
  # end

  defp fetch_contract_calls_indexes(create_txi) do
    contract_call_spec =
      Ex2ms.fun do
        {:contract_call, {^create_txi, call_txi}, :_, :_, :_, :_} -> call_txi
      end

    Model.ContractCall
    |> AeMdw.Db.Util.select(contract_call_spec)
    |> Enum.map(fn call_txi ->
      Model.tx(block_index: {height, mbi}) = Util.read_tx!(call_txi)

      %Aex9CallIndex{
        height: height,
        mbi: mbi,
        call_txi: call_txi
      }
    end)
  end

  defp reindex_account_aex9_mbs_txs(account2fix, height, calls_idxs) do
    {_key_block, micro_blocks} = AE.Db.get_blocks(height)
    mbi_list = Enum.map(calls_idxs, fn %Aex9CallIndex{mbi: mbi} -> mbi end)

    IO.puts("micro-blocks from calls #{inspect(mbi_list, charlists: :as_lists)}")

    micro_blocks
    |> Enum.with_index()
    |> Enum.filter(fn {_mb, mbi} -> mbi in mbi_list end)
    |> Enum.reduce(0, fn {mblock, mbi}, acc ->
      IO.puts("reindexing height #{height} micro-block #{mbi}")

      call_txi_list =
        calls_idxs
        |> Enum.filter(fn %Aex9CallIndex{mbi: call_mbi} -> call_mbi == mbi end)
        |> Enum.map(fn %Aex9CallIndex{call_txi: call_txi} -> call_txi end)

      acc + write_aex9_records_on_match(mblock, {height, mbi}, call_txi_list, account2fix)
    end)
  end

  defp write_aex9_records_on_match(
         micro_block,
         block_index,
         call_txi_list,
         %Account2Fix{
           account_pk: account_pk,
           contract_pk: contract_pk
         }
       ) do
    hash_recomputed = :aec_headers.hash_header(:aec_blocks.to_micro_header(micro_block)) |> ok!
    micro_block
    |> :aec_blocks.txs()
    |> Enum.map(fn signed_tx ->
      {mod, tx} = :aetx.specialize_callback(:aetx_sign.tx(signed_tx))
      {mod.type(), tx}
    end)
    |> Enum.filter(&tx_type_and_contract_match?(&1, contract_pk))
    |> Enum.map(fn {_type, tx} ->
      tx_raw_logs = contract_call_tx_logs(tx, contract_pk, block_index, hash_recomputed)
      # TODO: set proper txi for this tx (raw_logs)
      txi = List.first(call_txi_list)

      # raw_logs
      # |> find_txis_by_data_log(contract_pk)
      # |> Enum.filter(&same_micro_block?(&1, block_index))
      # |> group_raw_logs_by_txi(call_txi_list)
      # |> Enum.map(fn {txi, raw_logs} ->
      contract_id = Aex9Helper.enc_ct(contract_pk)
      account_id = Aex9Helper.enc_id(account_pk)
      IO.inspect(contract_id)
      IO.inspect(account_id)

      if DbContract.write_aex9_records(contract_pk, tx_raw_logs, txi, account_pk) do
        account_id = Aex9Helper.enc_id(account_pk)
        contract_id = Aex9Helper.enc_ct(contract_pk)
        IO.puts("Fixed account #{account_id} balance for #{contract_id}")
        1
      else
        IO.puts("Failed to write")
        0
      end
    end)
    |> Enum.sum()
  end

  defp tx_type_and_contract_match?({type, tx}, account_contract_pk) do
    type == :contract_call_tx and
      :aect_call_tx.contract_pubkey(tx) == account_contract_pk
  end

  defp contract_call_tx_logs(tx, contract_pk, block_index, hash_recomputed) do
    block_hash = block_index |> Util.read_block!() |> Model.block(:hash)

    if block_hash != hash_recomputed, do: IO.inspect "##### HASH DIFF ####"
    {_fun_arg_res, call_rec} =
      Contract.call_tx_info(tx, contract_pk, block_hash, &Contract.to_map/1)

    :aect_call.log(call_rec)
  end

  # defp find_txis_by_data_log(raw_logs, contract_pk) do
  #   create_txi = origin_txi(contract_pk)
  #   transfer_evt_hash = AeMdw.Node.aex9_transfer_event_hash()

  #   raw_logs
  #   |> Enum.filter(fn {_addr, [evt_hash | _args], _data} -> evt_hash == transfer_evt_hash end)
  #   |> Enum.map(fn {_addr, [evt_hash | _args], data} ->
  #     evt_spec =
  #       Ex2ms.fun do
  #         {:data_contract_log, {^data, call_txi, ^create_txi, ^evt_hash, :_}, :_} -> call_txi
  #       end

  #     case Util.select(Model.DataContractLog, evt_spec) do
  #       [] -> nil
  #       txi_list -> txi_list
  #     end
  #   end)
  #   |> List.flatten()
  # end

  # defp same_micro_block?(txi, mb_block_index) do
  #   m_tx = Util.read_tx!(txi)
  #   Model.tx(m_tx, :block_index) == mb_block_index
  # end

  defp normalized_amounts(amounts), do: Aex9Helper.normalize_balances(amounts)

  defp account_balances_is_empty?(account_pk, last_txi) do
    contracts =
      AeMdw.Db.Contract.aex9_search_contract(account_pk, last_txi)
      |> Map.to_list()
      |> Enum.sort_by(&elem(&1, 1), &<=/2)

    height_hash = DBN.top_height_hash(false)

    balance =
      contracts
      |> Enum.map(fn {contract_pk, txi} ->
        {amount, _} = DBN.aex9_balance(contract_pk, account_pk, height_hash)
        {amount, txi, contract_pk}
      end)
      |> Enum.map(&Aex9ControllerView.balance_to_map/1)

    Enum.empty?(balance)
  end
end
