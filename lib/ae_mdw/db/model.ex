defmodule AeMdw.Db.Model do
  require Record
  require Ex2ms

  alias AeMdw.Node, as: AE
  alias :aeser_api_encoder, as: Enc

  import Record, only: [defrecord: 2]
  import AeMdw.{Util, Db.Util}

  ################################################################################

  # txs table :
  #     index = tx_index (0..), id = tx_id
  @tx_defaults [index: -1, id: <<>>, block_index: {-1, -1}, time: -1]
  defrecord :tx, @tx_defaults

  # def tx(tx_index, tx_id, block_index),
  #   do: tx(index: tx_index, id: tx_id, block_index: block_index)

  # txs block index :
  #     index = {kb_index (0..), mb_index}, tx_index = tx_index, hash = block (header) hash
  #     if tx_index == nil -> txs not synced yet on that height
  #     if tx_index == -1  -> no tx occured yet
  #     On keyblock boundary: mb_index = -1}
  @block_defaults [index: {-1, -1}, tx_index: nil, hash: <<>>]
  defrecord :block, @block_defaults

  # txs time index :
  #     index = {mb_time_msecs (0..), tx_index = (0...)},
  @time_defaults [index: {-1, -1}, unused: nil]
  defrecord :time, @time_defaults

  # txs type index  :
  #     index = {tx_type, tx_index}
  @type_defaults [index: {nil, -1}, unused: nil]
  defrecord :type, @type_defaults

  # txs objects     :
  #     index = {tx_type, object_pubkey, tx_index}, id_tag = id_tag, role = role
  @object_defaults [index: {nil, nil, -1}, id_tag: nil, role: nil]
  defrecord :object, @object_defaults

  # object origin :
  #     index = {:contract | :name | ..., pubkey, tx_index}, tx_id = tx_hash
  @origin_defaults [index: {nil, nil, nil}, tx_id: nil]
  defrecord :origin, @origin_defaults

  # we need this one to quickly locate origin keys to delete for invalidating a fork
  #
  # rev object origin :
  #     index = {tx_index, :contract | :name | ..., pubkey}, unused: nil
  @rev_origin_defaults [index: {nil, nil, nil}, unused: nil]
  defrecord :rev_origin, @rev_origin_defaults

  # TODO:
  # contract events :
  #     index = {ct_address, event_name, tx_index, ct_address_log_local_id (0..)}, event = event
  @event_defaults [index: {"", nil, -1, -1}, event: nil]
  defrecord :event, @event_defaults
  # def event(contract_id, event_name, tx_index, ct_local_index, event),
  #   do: event([index: {contract_id, event_name, tx_index, ct_local_index}, event: event])

  def tables(),
    do: [
      AeMdw.Db.Model.Tx,
      AeMdw.Db.Model.Block,
      AeMdw.Db.Model.Time,
      AeMdw.Db.Model.Type,
      AeMdw.Db.Model.Object,
      AeMdw.Db.Model.Origin,
      AeMdw.Db.Model.RevOrigin,
      AeMdw.Db.Model.Event
    ]

  def records(),
    do: [:tx, :block, :time, :type, :object, :origin, :rev_origin, :event]

  def fields(record),
    do: for({x, _} <- defaults(record), do: x)

  def record(AeMdw.Db.Model.Tx), do: :tx
  def record(AeMdw.Db.Model.Block), do: :block
  def record(AeMdw.Db.Model.Time), do: :time
  def record(AeMdw.Db.Model.Type), do: :type
  def record(AeMdw.Db.Model.Object), do: :object
  def record(AeMdw.Db.Model.Origin), do: :origin
  def record(AeMdw.Db.Model.RevOrigin), do: :rev_origin
  def record(AeMdw.Db.Model.Event), do: :event

  def table(:tx), do: AeMdw.Db.Model.Tx
  def table(:block), do: AeMdw.Db.Model.Block
  def table(:time), do: AeMdw.Db.Model.Time
  def table(:type), do: AeMdw.Db.Model.Type
  def table(:object), do: AeMdw.Db.Model.Object
  def table(:origin), do: AeMdw.Db.Model.Origin
  def table(:rev_origin), do: AeMdw.Db.Model.RevOrigin
  def table(:event), do: AeMdw.Db.Model.Event

  def defaults(:tx), do: @tx_defaults
  def defaults(:block), do: @block_defaults
  def defaults(:time), do: @time_defaults
  def defaults(:type), do: @type_defaults
  def defaults(:object), do: @object_defaults
  def defaults(:origin), do: @origin_defaults
  def defaults(:rev_origin), do: @rev_origin_defaults
  def defaults(:event), do: @event_defaults

  ##########

  def block_to_raw_map({:block, {_kbi, mbi}, _txi, hash}),
    do: record_to_map(:aec_db.get_header(hash), AE.hdr_fields((mbi == -1 && :key) || :micro))

  def block_to_map({:block, {_kbi, _mbi}, _txi, hash}) do
    header = :aec_db.get_header(hash)
    :aec_headers.serialize_for_client(header, prev_block_type(header))
  end

  ##########

  def tx_record_to_map(tx_type, tx_rec) do
    AeMdw.Node.tx_fields(tx_type)
    |> Stream.with_index(1)
    |> Enum.reduce(
      %{},
      fn {field, pos}, acc ->
        put_in(acc[field], elem(tx_rec, pos))
      end
    )
  end

  def tx_to_raw_map({:tx, _index, hash, {_kb_index, _mb_index}, _mb_time} = rec),
    do: tx_to_raw_map(rec, tx_rec_data(hash))

  def tx_to_raw_map(
        {:tx, index, hash, {kb_index, mb_index}, mb_time},
        {block_hash, type, signed_tx, tx_rec}
      ) do
    tx_map = tx_record_to_map(type, tx_rec) |> put_in([:type], type)

    raw = %{
      block_hash: block_hash,
      signatures: :aetx_sign.signatures(signed_tx),
      hash: hash,
      block_height: kb_index,
      micro_index: mb_index,
      micro_time: mb_time,
      tx_index: index,
      tx: tx_map
    }

    custom_raw_data(type, raw, tx_rec)
  end

  end

  def tx_to_map({:tx, index, hash, {_kb_index, mb_index}, mb_time}) do
    {block_hash, signed_tx} = :aec_db.find_tx_with_location(hash)
    {type, _} = :aetx.specialize_type(:aetx_sign.tx(signed_tx))

  def tx_to_map({:tx, _index, hash, {_kb_index, _mb_index}, _mb_time} = rec),
    do: tx_to_map(rec, tx_rec_data(hash))

  def tx_to_map(
        {:tx, index, hash, {_kb_index, mb_index}, mb_time},
        {block_hash, type, signed_tx, tx_rec}
      ) do
    header = :aec_db.get_header(block_hash)
    enc_tx = :aetx_sign.serialize_for_client(header, signed_tx)

    custom_encode(type, enc_tx)
    |> put_in(["tx_index"], index)
    |> put_in(["micro_index"], mb_index)
    |> put_in(["micro_time"], mb_time)
  end

  def custom_encode(:oracle_response_tx, tx),
    do: update_in(tx, ["tx", "response"], &maybe_base64/1)

  def custom_encode(_, tx),
    do: tx

  def maybe_base64(bin) do
    try do
      dec = :base64.decode(bin)
      (String.valid?(dec) && dec) || bin
    rescue
      _ -> bin
    end
  end
end
