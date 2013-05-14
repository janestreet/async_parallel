(* OASIS_START *)
(* OASIS_STOP *)

let dispatch = function
  | Before_options ->
    Options.make_links := false
  | _ ->
    ()

let () = Ocamlbuild_plugin.dispatch (fun hook -> dispatch hook; dispatch_default hook)
