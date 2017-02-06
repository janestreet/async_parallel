open Core
open Async
open Async_parallel_deprecated.Std

let p s = Core.Printf.printf "%s: %s\n%!" (Pid.to_string (Unix.getpid ())) s

let foo h =
  Pipe.iter_without_pushback (Hub.listen_simple h) ~f:(fun (id, `Ping) ->
    p "read ping";
    Hub.send h id `Pong;
    p "sent pong")
  >>| fun () -> `Done
;;

let main () =
  Parallel.spawn ~where:Parallel.random foo >>> fun (c, _res) ->
  let rec loop () =
    Channel.write c `Ping;
    Channel.read c >>> fun `Pong ->
    Clock.after (sec 1.) >>> loop
  in
  loop ();
  Clock.after (sec 60.) >>> fun () -> Shutdown.shutdown 0
;;

let () =
  Exn.handle_uncaught ~exit:true (fun () ->
    Parallel.init ~cluster:
      {Cluster.master_machine = Unix.gethostname ();
       worker_machines = ["hkg-qws-r01"; "hkg-qws-r02"]} ();
    main ();
    never_returns (Scheduler.go ()))
;;
