{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
  buildInputs = with pkgs.python311Packages; [
    pkgs.python311
    numpy
    pandas
    requests
    tqdm
    
    # Dev dependencies
    black
  ];
}