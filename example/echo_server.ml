open Core
open Async
open Async_parallel_deprecated.Std

let p s = Core.Printf.printf "%s: %s\n%!" (Pid.to_string (Unix.getpid ())) s

let master = Unix.gethostname ()

let echo_server s =
  p "echo server running";
  Deferred.create (fun i ->
    Pipe.iter' (Hub.listen s) ~f:(fun q ->
      Deferred.all_unit
        (Queue.fold q ~init:[] ~f:(fun acc x ->
          match x with
          | `Connect _ ->
            p "client connected";
            Deferred.unit :: acc
          | `Data (c, `Echo a) ->
            p "client sent echo request";
            Hub.send s c (`Echo a);
            Hub.flushed s :: acc
          | `Data (_, `Die) ->
            p "client sent die request";
            Ivar.fill i ();
            Deferred.unit :: acc
          | `Disconnect _ -> Deferred.unit :: acc)))
    >>> fun () -> Ivar.fill_if_empty i ())
;;

let main () =
  Parallel.spawn ~where:Parallel.random echo_server >>> fun (c, res) ->
  Channel.write c (`Echo "foo");
  Channel.read c >>> fun (`Echo z) ->
  assert (z = "foo");
  p "starting more clients";
  Deferred.create (fun iv ->
    let rec loop i =
      if i > 9999 then Ivar.fill iv ()
      else begin
        p "starting client";
        Parallel.run ~where:Parallel.round_robin (fun () ->
          p "second client started, sending echo request with same channel!";
          let id = Int.to_string i in
          Channel.write c (`Echo id);
          Channel.read c >>= fun (`Echo z) ->
          assert (z = id);
          p "creating a worker within a worker";
          Parallel.run ~where:Parallel.round_robin (fun () ->
            p "worker from worker created";
            Channel.write c (`Echo ("sub" ^ id));
            p "worker from worker channel written";
            Channel.read c >>= (fun (`Echo z) ->
              p "worker from worker read echo";
              assert (z = "sub" ^ id);
              p "worker from worker assert succeeded";
              Deferred.unit))
          >>| (function
          | Error e -> p (sprintf "worker within worker died %s" e)
          | Ok () -> p "read worker from worker's result successfully"))
        >>> (function
        | Error e -> p (sprintf "client died with exception \"%s\"" e)
        | Ok () ->
          p "read client's result successfully looping";
          loop (i + 1))
      end
    in
    p "calling loop";
    loop 0)
  >>> fun () ->
  p "second client done, writing die";
  Channel.write c `Die;
  res >>> function
  | Error e -> p (sprintf "echo server died with exception %s" e)
  | Ok () -> p "echo server dead"; Shutdown.shutdown 0
;;

let () =
  p "starting the world";
  Exn.handle_uncaught ~exit:true (fun () ->
    p "calling Parallel.init";
    Parallel.init ~cluster:
      {Cluster.
       master_machine = master;
       (* Though not exactly recommended this is an example of how flexible the
          new library is. *)
       worker_machines = []}
      ();
    p "calling main";
    main ();
    p "calling scheduler go";
    never_returns (Scheduler.go ()))
;;
