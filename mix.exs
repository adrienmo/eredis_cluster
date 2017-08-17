defmodule EredisCluster.Mixfile do
  use Mix.Project

  @version File.read!("VERSION") |> String.strip

  def project do
    [app: :eredis_cluster,
     version: @version,
     description: "An erlang wrapper for eredis library to support cluster mode",
     package: package,
     deps: deps]
  end

  def application do
    [mod: {:eredis_cluster, []},
     applications: [:eredis, :poolboy]
    ]
  end

  defp deps do
    [{:poolboy, "~> 1.5.1"},
      {:eredis, "~> 1.1.0"}]
  end

  defp package do
    [files: ~w(include src mix.exs rebar.config README.md LICENSE VERSION),
     maintainers: ["Adrien Moreau"],
     licenses: ["MIT"],
     links: %{"GitHub" => "https://github.com/adrienmo/eredis_cluster"}]
  end
end
