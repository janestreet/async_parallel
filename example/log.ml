open Core
open Async
open Async_parallel_deprecated.Std
module Glog = Log.Global

let p s = Glog.info "%s %s: %s\n%!" (Unix.gethostname ()) (Pid.to_string (Unix.getpid ())) s

let foo () =
  p "solving...";
  Clock.after (sec 1.) >>| fun () -> "bar"
;;

let main () =
  Parallel.run ~where:(`On "machine1") foo >>> function
  | Error e -> p (sprintf "died with exception %s" e)
  | Ok str ->
    p (sprintf "main process gets the result: %s" str);
    Shutdown.shutdown 0
;;

let () =
  Exn.handle_uncaught ~exit:true (fun () ->
    Parallel.init ~cluster:
      {Cluster.master_machine = Unix.gethostname ();
       worker_machines = ["machine1"; "machine2"]} ();
    p "calling main";
    main ();
    p "calling scheduler go";
    never_returns (Scheduler.go ()))
;;
