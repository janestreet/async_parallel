open Core
module U = Unix

module Socket_file : sig
  type t [@@deriving sexp_of]

  include Stringable with type t := t
end = String

module Debug = Async_kernel.Async_kernel_private.Debug

let debug = Debug.parallel

let oc = lazy (Out_channel.create (sprintf "/tmp/%d.log" (Pid.to_int (Unix.getpid ()))))

let dbp msg =
  let oc = Lazy.force oc in
  Out_channel.output_string oc msg;
  Out_channel.newline oc;
  Out_channel.flush oc

let my_ip = lazy begin
  try
    (match U.Host.getbyname (U.gethostname ()) with
    | None -> U.Inet_addr.localhost
    | Some host ->
      let addrs =
        Array.filter host.U.Host.addresses ~f:(fun a -> a <> U.Inet_addr.localhost)
      in
      if Array.length addrs = 0 then
        U.Inet_addr.localhost
      else
        addrs.(0))
  with _ -> U.Inet_addr.localhost
end

module Cluster = struct
  type t = {
    master_machine: string; (* DNS name of the machine that will start it all *)
    worker_machines: string list; (* DNS name of worker machines *)
  } [@@deriving sexp, bin_io]
end

let socket_connect_inet socket (addr, port) =
  let addr =
    if U.Inet_addr.(=) addr (Lazy.force my_ip)
    then begin
      U.Inet_addr.localhost
    end
    else addr
  in
  Async.Socket.connect socket (`Inet (addr, port))
