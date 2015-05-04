open Core.Std
open Async.Std
open Async_parallel.Std

let wide () =
  Deferred.List.iter [ 1; 2; 10; 100 ] ~f:(fun num_children ->
    Printf.printf "creating: %d\n%!" num_children;
    Deferred.all
      (List.init num_children ~f:(fun i ->
        Parallel.run (fun () ->
          Printf.printf "i: %d\n%!" i;
          return i)
        >>| function
        | Error e -> failwith e
        | Ok i -> i))
    >>| fun l ->
    Printf.printf "done!\n%!";
    assert (l = List.init num_children ~f:Fn.id);
    Printf.printf "assert ok\n%!")
;;

let () =
  Parallel.init ();
  (wide () >>> fun () ->
   Printf.printf "shutdown\n%!";
   Shutdown.shutdown 0);
  never_returns (Scheduler.go ())
;;
