defmodule Baobab.MixProject do
  use Mix.Project

  def project do
    [
      app: :baobab,
      version: "0.31.0",
      elixir: "~> 1.18",
      name: "Baobab",
      source_url: "https://github.com/mwmiller/baobab_ex",
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      description: description(),
      package: package(),
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:basex, ">= 0.0.0"},
      {:ed25519, "~> 1.4"},
      {:lipmaa, ">= 1.1.0"},
      {:varu64, "~> 1.0.0"},
      {:yamfhash, ">= 1.0.0"},
      # Not written by me
      {:enacl, "~> 1.2"},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false}
    ]
  end

  defp description do
    """
    Bamboo append-only logs
    """
  end

  defp package do
    [
      files: ["lib", "mix.exs", "README*", "LICENSE*"],
      maintainers: ["Matt Miller"],
      licenses: ["MIT"],
      links: %{
        "GitHub" => "https://github.com/mwmiller/baobab_ex",
        "Spec" => "https://github.com/AljoschaMeyer/bamboo"
      }
    ]
  end
end
