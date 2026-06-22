# shell.nix
{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
  nativeBuildInputs = with pkgs; [
    rustup
    clang
    lld
    pkg-config
    maturin
  ];

  shellHook = ''
    export PATH="$HOME/.cargo/bin:$PATH"
    export CC=clang
    export CXX=clang++
    export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=clang
    export RUSTFLAGS="-C link-arg=-fuse-ld=lld"
  '';
}
