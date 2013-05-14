open Core.Std
module U = Unix

module Socket_file : sig
  type t with sexp_of

  include Stringable with type t := t
end = String

module Debug = Async_core.Debug

let debug = Debug.parallel

let oc = lazy (Out_channel.create (sprintf "/tmp/%d.log" (Pid.to_int (Unix.getpid ()))))

let dbp msg =
  let oc = Lazy.force oc in
  Out_channel.output_string oc msg;
  Out_channel.newline oc;
  Out_channel.flush oc

exception Cant_determine_our_ip_address
let my_ip = lazy begin
  match U.Host.getbyname (U.gethostname ()) with
  | None -> raise Cant_determine_our_ip_address
  | Some host ->
    let addrs =
      Array.filter host.U.Host.addresses ~f:(fun a -> a <> U.Inet_addr.localhost)
    in
    if Array.length addrs = 0 then
      raise Cant_determine_our_ip_address
    else
      addrs.(0)
end

module Cluster = struct
  type t = {
    master_machine: string; (* DNS name of the machine that will start it all *)
    worker_machines: string list; (* DNS name of worker machines *)
  } with sexp, bin_io
end
