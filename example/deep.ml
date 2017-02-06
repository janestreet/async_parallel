open Core
open Async
open Async_parallel_deprecated.Std

let deep () =
  Deferred.List.iter [ 1; 2; 10; 100 ] ~f:(fun depth ->
    let rec loop i =
      if i = 0
      then return 0
      else begin
        Parallel.run (fun () -> loop (i - 1))
        >>| function
        | Error e -> failwith e
        | Ok j -> j + 1
      end
    in
    loop depth
    >>| fun d ->
    assert (d = depth))
;;

let () =
  Parallel.init ();
  (deep () >>> fun () -> Shutdown.shutdown 0);
  never_returns (Scheduler.go ())
;;
