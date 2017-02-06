open Core
open Async
open Async_parallel_deprecated.Std

let wide () =
  Deferred.List.iter [ 1; 2; 10; 100 ] ~f:(fun num_children ->
    Deferred.all
      (List.init num_children ~f:(fun i ->
        Parallel.run (fun () -> return i)
        >>| function
        | Error e -> failwith e
        | Ok i -> i))
    >>| fun l ->
    assert (l = List.init num_children ~f:Fn.id))
;;

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

let fib () =
  let rec fib n =
    Parallel.run (fun () ->
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

let ring () =
  let add c f =
    Parallel.spawn (fun h ->
      let ready = Ivar.create () in
      (Pipe.iter_without_pushback (Hub.listen h) ~f:(function
      | `Connect _ -> Ivar.fill_if_empty ready ()
      | _ -> ()) >>> ignore);
      let rec loop () =
        Channel.read c >>> fun a ->
        f a >>> fun b ->
        Ivar.read ready >>> fun () ->
        Hub.send_to_all h b;
        Hub.flushed h
        >>> fun () ->
        loop ()
      in
      loop ();
      Deferred.never ())
    >>| fun (c, _) -> c
  in
  let main () =
    Parallel.hub () >>= fun hub ->
    let ready = Ivar.create () in
    (Pipe.iter_without_pushback (Hub.listen hub) ~f:(function
    | `Connect _ -> Ivar.fill_if_empty ready ()
    | _ -> ()) >>> ignore);
    Hub.open_channel hub >>= fun c ->
    add c (fun x -> return (x + 1)) >>= fun c ->
    add c (fun x -> return (x + 2)) >>= fun c ->
    add c (fun x -> return (x + 3)) >>= fun c ->
    let rec loop i =
      if i >= 1000 then Deferred.unit
      else begin
        let res = Channel.read c in
        Ivar.read ready >>= fun () ->
        Hub.send_to_all hub i;
        Hub.flushed hub
        >>= fun () ->
        res >>= fun i ->
        loop (i + 4)
      end
    in
    loop 0 >>= fun () ->
    Hub.close hub
  in
  main ()
;;

let echo_server () =
  let echo_server s =
    Deferred.create (fun i ->
      Pipe.iter' (Hub.listen s) ~f:(fun q ->
        Deferred.all_unit
          (Queue.fold q ~init:[] ~f:(fun acc x ->
            match x with
            | `Connect _ ->
              Deferred.unit :: acc
            | `Data (c, `Echo a) ->
              Hub.send s c (`Echo a);
              Hub.flushed s :: acc
            | `Data (_, `Die) ->
              Ivar.fill i ();
              Deferred.unit :: acc
            | `Disconnect _ -> Deferred.unit :: acc)))
      >>> fun () -> Ivar.fill_if_empty i ())
  in
  let main () =
    Parallel.spawn echo_server >>= fun (c, res) ->
    Channel.write c (`Echo "foo");
    Channel.read c >>= fun (`Echo z) ->
    assert (z = "foo");
    Deferred.create (fun iv ->
      let rec loop i =
        if i >= 100 then Ivar.fill iv ()
        else begin
          Parallel.run (fun () ->
            let id = Int.to_string i in
            Channel.write c (`Echo id);
            Channel.read c >>= fun (`Echo z) ->
            assert (z = id);
            let go () =
              Parallel.run (fun () ->
                Channel.write c (`Echo ("sub" ^ id));
                Channel.read c >>= (fun (`Echo z) ->
                  assert (z = "sub" ^ id);
                  Deferred.unit))
              >>| (function
              | Error e -> failwithf "worker within worker died %s" e ()
              | Ok () -> ())
            in
            Deferred.all_unit [go (); go (); go (); go ()])
          >>> (function
          | Error e -> failwithf "client died with exception \"%s\"" e ()
          | Ok () -> loop (i + 1))
        end
      in
      loop 0)
    >>= fun () ->
    Channel.write c `Die;
    res >>| function
    | Error e -> failwithf "echo server died with exception %s" e ()
    | Ok () -> ()
  in
  main ()
;;

(* We can't test remote machines within the test framework because qtest.exe doesn't
   support being called in that way, so there is a separate binary, test_remote.exe, for
   testing remote machine functionality. *)
let remote () =
  Unix.readlink (sprintf "/proc/%d/exe" (Pid.to_int (Unix.getpid ())))
  >>= fun our_bin ->
  let our_dir = Filename.dirname our_bin in
  Unix.system_exn (sprintf "%s/test_remote.exe" our_dir)

let init () =
  (* Before calling [Parallel.create], we force creation of the async scheduler, so
     we can test its ability to reset in the forked master process. *)
  ignore (Lazy.force Writer.stdout);
  assert (not (Scheduler.is_ready_to_initialize ()));
  Parallel.init ~fail_if_async_has_been_initialized:false ();
  Deferred.unit
;;

let shutdown () = Parallel.shutdown () >>| Or_error.ok_exn

let tests =
  [ "Parallel_test", (fun () ->
      init () >>= fun () ->
      Deferred.all_unit [
        wide ();
        deep ();
        fib ();
        ring ();
        echo_server ();
        (* remote (); *)
      ] >>= fun () ->
      shutdown ())
  ]
;;
