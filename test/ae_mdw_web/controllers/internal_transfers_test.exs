defmodule AeMdwWeb.InternalTransfersTest do
  use AeMdwWeb.ConnCase

  alias AeMdwWeb.Continuation, as: Cont
  alias AeMdwWeb.TestUtil

  import AeMdwWeb.Util
  import AeMdw.Db.Util

  @cont_table_name AeMdwWeb.Continuation
  @default_limit 10
  @default_page 1
  @default_range "300000-310000"
  @default_fun :transfers
  @default_mod AeMdwWeb.TransferController

  describe "transfer" do
    @tag :testing
    test "get internal transfers by generations range", %{conn: conn} do
      range = @default_range
      limit = @default_limit
      page = @default_page
      offset = calculate_offset(page, limit)
      conn = get(conn, "/transfers/gen/#{range}?page=#{page}&limit=#{limit}")

      json_response = json_response(conn, 200)

      local_response =
        TestUtil.handle_input(fn ->
          get_transfers(
            {@default_mod, @default_fun, %{}, {:gen, str_to_range(range)}, offset},
            limit,
            page,
            range
          )
        end)

      assert json_response["data"] == local_response["data"]
      assert json_response["next"] == local_response["next"]
    end
  end

  @tag :testing
  test "get consequent page of internal transfers by generations range", %{conn: conn} do
    # first request, just needed to fingerprint it in db
    range = @default_range
    limit = @default_limit
    page = @default_page
    offset = calculate_offset(page, limit)
    conn = get(conn, "/transfers/gen/#{range}?page=#{page}&limit=#{limit}")

    _json_response = json_response(conn, 200)

    # second actual request with incremented page
    range = @default_range
    limit = @default_limit
    next_page = @default_page + 1
    offset = calculate_offset(next_page, limit)

    conn = get(conn, "/transfers/gen/#{range}?page=#{next_page}&limit=#{limit}")

    json_response = json_response(conn, 200)

    local_response =
      TestUtil.handle_input(fn ->
        get_transfers(
          {@default_mod, @default_fun, %{}, {:gen, str_to_range(range)}, offset},
          limit,
          next_page,
          range
        )
      end)

    assert json_response["data"] == local_response["data"]
    assert json_response["next"] == local_response["next"]
  end

  @tag :testing
  test "get a page of internal transfers by same generations range with set limit", %{conn: conn} do
    range = @default_range
    limit = @default_limit + @default_limit
    page = @default_page
    conn = get(conn, "/transfers/gen/#{range}?page=#{page}&limit=#{limit}")
    offset = calculate_offset(page, limit)

    json_response = json_response(conn, 200)

    local_response =
      TestUtil.handle_input(fn ->
        get_transfers(
          {@default_mod, @default_fun, %{}, {:gen, str_to_range(range)}, offset},
          limit,
          page,
          range
        )
      end)

    assert json_response["data"] == local_response["data"]
    assert json_response["next"] == local_response["next"]
    assert Enum.count(json_response["data"]) == Enum.count(local_response["data"])
  end

  @tag :testing
  test "renders empty response for non-existing generation", %{conn: conn} do
    limit = @default_limit
    page = @default_page
    invalid_height = current_height + 10
    offset = calculate_offset(page, limit)

    header =
      case :aec_chain.top_block() do
        {:mic_block, header, _txs, _} -> header
        {:key_block, header} -> header
      end

    current_height = :aec_headers.height(header)
    range = range_to_str(current_height..invalid_height)

    conn = get(conn, "/transfers/gen/#{range}?page=#{page}&limit=#{limit}")
    json_response = json_response(conn, 200)

    local_response =
      TestUtil.handle_input(fn ->
        get_transfers(
          {@default_mod, @default_fun, %{}, {:gen, str_to_range(range)}, offset},
          limit,
          page,
          range
        )
      end)

    assert json_response["data"] == local_response["data"]
    assert json_response["next"] == local_response["next"]
  end

  @tag :testing
  test "renders error caused by random access which is not supported", %{conn: conn} do
    parsed_range = str_to_range(@default_range)
    range = range_to_str(Range.new(parsed_range.first + 100_000, parsed_range.last + 100_000))
    limit = @default_limit
    page = @default_page + 10

    offset = calculate_offset(page, limit)
    conn = get(conn, "/transfers/gen/#{range}?page=#{page}&limit=#{limit}")

    json_response = json_response(conn, 400)
    local_response = %{"error" => "random access not supported"}

    assert json_response == local_response
  end

  @tag :testing
  test "get internal transfers of a specific kind, towards the chain", %{conn: conn} do
    direction = "forward"
    kind = "reward_block"
    page = @default_page
    limit = @default_limit
    parsed_range = str_to_range(@default_range)
    range = range_to_str(Range.new(parsed_range.first + 100_000, parsed_range.last + 100_000))
    offset = calculate_offset(page, limit)
    conn = get(conn, "/transfers/#{direction}?kind=#{kind}page=#{page}&limit=#{limit}")

    json_response = json_response(conn, 200)

    local_response =
      TestUtil.handle_input(fn ->
        get_transfers(
          {@default_mod, @default_fun, %{}, {:gen, str_to_range(range)}, offset},
          limit,
          page,
          range
        )
      end)

    assert Enum.count(json_response) == limit
    assert json_response[""] == local_response[""]
  end

  test "get internal transfers of a specific kind, backwards the chain", %{conn: conn} do
    direction = "backward"
    kind = "reward_dev"
    page = @default_page
    limit = @default_limit

    conn = get(conn, "/transfers/#{direction}?kind=#{kind}page=#{page}&limit=#{limit}")
  end

  # ################

  defp calculate_offset(page, limit), do: (page - 1) * limit

  defp get_transfers(cont_key, limit, page, range) do
    {:ok, data, has_cont} = Cont.response_data(cont_key, limit)

    parse(data, {limit, page, range}, has_cont)
  end

  defp str_to_range(str) do
    [first, last] = String.split(str, "-")
    Range.new(String.to_integer(first), String.to_integer(last))
  end

  defp range_to_str(%Range{first: first, last: last}) do
    "#{first}-#{last}"
  end

  defp parse(data, _, false) do
    {:ok, resp} =
      Jason.Decoder.parse(
        Jason.encode!(%{
          data: data,
          next: nil
        }),
        %{keys: :strings, strings: :copy}
      )

    resp
  end

  defp parse(data, {limit, page, range}, true) do
    {:ok, resp} =
      Jason.Decoder.parse(
        Jason.encode!(%{
          data: data,
          next: "transfers/gen/#{range}?limit=#{limit}&page=#{page + 1}"
        }),
        %{keys: :strings, strings: :copy}
      )

    resp
  end
end
