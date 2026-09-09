defmodule KafkaManagerWeb.PageController do
  use KafkaManagerWeb, :controller

  def home(conn, _params) do
    render(conn, :home)
  end
end
