# Integration tests need a running broker. Point FIREMQ_URL at it to run them, for example
# FIREMQ_URL=fire://guest:guest@localhost:5673 mix test
exclude = if System.get_env("FIREMQ_URL") in [nil, ""], do: [:integration], else: []

ExUnit.start(exclude: exclude)
