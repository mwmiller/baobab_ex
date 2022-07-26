defmodule Baobab.MixProject do
  use Mix.Project

  def project do
    [
      app: :baobab,
      version: "0.1.0",
      elixir: "~> 1.13",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:basex, ">= 0.0.0"},
      {:ed25519, "~> 1.4"},
      {:lipmaa, ">= 0.0.0"},
      {:varu64, "~> 0.2.0"},
      {:yamfhash, ">= 0.0.0"}
    ]
  end
end
