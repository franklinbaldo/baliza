# To learn more about how to use Nix to configure your environment
# see: https://firebase.google.com/docs/studio/customize-workspace
{ pkgs, ... }: {
  # Which nixpkgs channel to use.
  channel = "stable-24.05"; # or "unstable"

  # Use https://search.nixos.org/packages to find packages
  packages = [
    pkgs.python312
    pkgs.uv
    pkgs.git
    pkgs.curl
    pkgs.sqlite
    pkgs.duckdb
  ];

  # Sets environment variables in the workspace
  env = {
    DATABASE_PATH = "data/baliza.duckdb";
    PYTHONPATH = "src";
  };
  
  idx = {
    # Search for the extensions you want on https://open-vsx.org/ and use "publisher.id"
    extensions = [
      "ms-python.python"
      "ms-python.black-formatter"
      "charliermarsh.ruff"
      "ms-toolsai.jupyter"
    ];

    # Enable previews
    previews = {
      enable = true;
      previews = {
        web = {
          command = ["uv" "run" "baliza" "ui"];
          manager = "web";
          env = {
            PORT = "$PORT";
          };
        };
      };
    };

    # Workspace lifecycle hooks
    workspace = {
      # Runs when a workspace is first created
      onCreate = {
        install-deps = "uv sync";
        init-project = "uv run baliza init";
      };
      # Runs when the workspace is (re)started
      onStart = {
        # Nothing needed for startup
      };
    };
  };
}
