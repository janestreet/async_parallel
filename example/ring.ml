open Core
open Async
open Async_parallel_deprecated.Std

let p s = Core.Printf.printf "%s: %s\n%!" (Pid.to_string (Unix.getpid ())) s

let add c f =
  (* Run the ring nodes on either of these two machines, but never on the master. *)
  Parallel.spawn ~where:(Parallel.random_in ["hkg-qws-r02"; "hkg-qws-r01"]) (fun h ->
    Pipe.iter_without_pushback (Hub.listen h) ~f:ignore
    >>> (fun () -> p "hub closed");
    let rec loop () =
      Channel.read c >>> fun a ->
      f a >>> fun b ->
      Hub.send_to_all h b;
      Hub.flushed h
      >>> fun () ->
      loop ()
    in
    loop ();
    Deferred.never ())
  >>| fun (c, _) -> c
;;

let main () =
  p "creating hub";
  Parallel.hub () >>> fun hub ->
  Pipe.iter_without_pushback (Hub.listen hub) ~f:ignore
  >>> (fun () -> p "main hub closed");
  p "hub created";
  Hub.open_channel hub >>> fun c ->
  p "channel of hub created";
  add c (fun x -> p (sprintf "adding 1 to %d" x); return (x + 1)) >>> fun c ->
  p "first process added";
  add c (fun x -> p (sprintf "adding 2 to %d" x); return (x + 2)) >>> fun c ->
  p "second process added";
  add c (fun x -> p (sprintf "adding 3 to %d" x); return (x + 3)) >>> fun c ->
  p "third process added";
  let rec loop i =
    if i < 1_000_000 then begin
      let res = Channel.read c in
      Hub.send_to_all hub i;
      Hub.flushed hub
      >>> fun () ->
      p "sent";
      res >>> fun i ->
      p (sprintf "adding 4 to %d" i);
      loop (i + 4)
    end else
      Pervasives.exit 0
  in
  after (sec 1.) >>> fun () ->
  loop 0
;;

let () =
  Parallel.init ~cluster:
    {Cluster.master_machine = Unix.gethostname ();
     worker_machines = ["hkg-qws-r01"; "hkg-qws-r02"]} ();
  main ();
  never_returns (Scheduler.go ())
;;
