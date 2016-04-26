#use "topfind";;
#require "js-build-tools.oasis2opam_install";;

open Oasis2opam_install;;

generate ~package:"async_parallel"
  [ oasis_lib "async_parallel_deprecated"
  ; file "META" ~section:"lib"
  ]
