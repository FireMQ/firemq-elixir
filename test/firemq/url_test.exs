defmodule FireMQ.URLTest do
  use ExUnit.Case, async: true

  alias FireMQ.URL

  doctest FireMQ.URL

  test "defaults fill in" do
    assert {:ok, url} = URL.parse("fire://localhost")
    assert url.transport == :tcp
    assert url.host == "localhost"
    assert url.port == 5673
    assert url.vhost == "/"
    assert {url.user, url.pass} == {"guest", "guest"}
  end

  test "credentials and vhost are percent-decoded" do
    assert {:ok, url} = URL.parse("fires://app:p%40ss@mq.example.com:7000/prod%2Feu")
    assert url.transport == :tls
    assert url.host == "mq.example.com"
    assert url.port == 7000
    assert {url.user, url.pass} == {"app", "p@ss"}
    assert url.vhost == "prod/eu"
  end

  test "an empty path, or a single slash, means vhost /" do
    assert {:ok, %URL{vhost: "/"}} = URL.parse("fire://localhost/")
    assert {:ok, %URL{vhost: "/"}} = URL.parse("fire://localhost/%2F")
    assert {:ok, %URL{vhost: "dev"}} = URL.parse("fire://localhost/dev")
  end

  test "a user without a password gets the default password" do
    assert {:ok, %URL{user: "app", pass: "guest"}} = URL.parse("fire://app@localhost")
    assert {:ok, %URL{user: "app", pass: ""}} = URL.parse("fire://app:@localhost")
  end

  test "the fires default port is 5674" do
    assert {:ok, %URL{transport: :tls, port: 5674}} = URL.parse("fires://localhost")
  end

  test "websocket URLs use the path as endpoint and the query for the vhost" do
    assert {:ok, url} = URL.parse("ws://localhost:15672/fire?vhost=dev")
    assert {url.transport, url.path, url.vhost} == {:ws, "/fire", "dev"}

    assert {:ok, url} = URL.parse("wss://mq.example.com")
    assert {url.port, url.path, url.vhost} == {15671, "/fire", "/"}
  end

  test "IPv6 hosts lose their brackets" do
    assert {:ok, url} = URL.parse("fire://[::1]:5673")
    assert url.host == "::1"
    assert URL.connect_host(url) == {0, 0, 0, 0, 0, 0, 0, 1}
  end

  test "other schemes are rejected" do
    assert {:error, %FireMQ.Error{kind: :url}} = URL.parse("amqp://localhost")
    assert {:error, %FireMQ.Error{kind: :url}} = URL.parse("localhost:5673")
  end

  test "inspect hides the password" do
    {:ok, url} = URL.parse("fire://app:secret@localhost")
    refute inspect(url) =~ "secret"
  end
end
