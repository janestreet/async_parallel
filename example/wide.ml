open Core
open Async
open Async_parallel_deprecated.Std

let wide () =
  Deferred.List.iter [ 1; 2; 10; 100 ] ~f:(fun num_children ->
    Core.Printf.printf "creating: %d\n%!" num_children;
    Deferred.all
      (List.init num_children ~f:(fun i ->
        Parallel.run (fun () ->
          Core.Printf.printf "i: %d\n%!" i;
          return i)
        >>| function
        | Error e -> failwith e
        | Ok i -> i))
    >>| fun l ->
    Core.Printf.printf "done!\n%!";
    assert (l = List.init num_children ~f:Fn.id);
    Core.Printf.printf "assert ok\n%!")
;;

let () =
  Parallel.init ();
  (wide () >>> fun () ->
   Core.Printf.printf "shutdown\n%!";
   Shutdown.shutdown 0);
  never_returns (Scheduler.go ())
;;
