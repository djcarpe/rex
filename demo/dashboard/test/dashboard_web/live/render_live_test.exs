defmodule DashboardWeb.RenderLiveTest do
  use DashboardWeb.ConnCase

  test "GET / renders the compute dashboard", %{conn: conn} do
    conn = get(conn, ~p"/")
    assert html_response(conn, 200) =~ "Compute nodes"
  end
end
