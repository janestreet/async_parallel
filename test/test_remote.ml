open Core
open Async
open Async_parallel_deprecated.Std

let wide () =
  Deferred.List.iter [ 1; 2; 10; 100 ] ~f:(fun num_children ->
    Deferred.all
      (List.init num_children ~f:(fun i ->
        Parallel.run ~where:Parallel.random (fun () -> return i)
        >>| function
        | Error e -> failwith e
        | Ok i -> i))
    >>| fun l ->
    assert (l = List.init num_children ~f:Fn.id))
;;

let fib () =
  let rec fib n =
    Parallel.run ~where:Parallel.round_robin (fun () ->
      match n with
      | 0 -> return 0
      | 1 -> return 1
      | n ->
        fib (n - 1)
        >>= fun f1 ->
        fib (n - 2)
        >>= fun f2 ->
        return (f1 + f2))
    >>| function
    | Error e -> failwith e
    | Ok n -> n
  in
  fib 10
  >>| fun n ->
  assert (n = 55)
;;

let main () =
  Parallel.init ~cluster:
    {Cluster.master_machine = Unix.gethostname ();
     worker_machines = ["localhost"]} ();
  Deferred.all_unit [ wide (); fib () ] >>> fun () ->
  Parallel.shutdown () >>> fun _ -> Shutdown.shutdown 0
;;

let () =
  main ();
  never_returns (Scheduler.go ())
