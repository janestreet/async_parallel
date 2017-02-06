open Core
open Async
open Import

let master_pid = ref None

let init ?cluster ?close_stdout_and_stderr ?(fail_if_async_has_been_initialized = true) () =
  if fail_if_async_has_been_initialized && not (Scheduler.is_ready_to_initialize ()) then
    failwith "Parallel.init called after async was initialized";
  master_pid := Some (Master_process.init ?cluster ?close_stdout_and_stderr ());
;;

let shutdown () =
  if debug then dbp "Intf.shutdown called";
  match !master_pid with
  | None -> return (Error (Error.of_string "\
Called Parallel.shutdown incorrectly.
Either parallel isn't running or we're not the main process.
"))
  | Some master_pid ->
    Master_process.shutdown ()
    >>= fun () ->
    Unix.waitpid master_pid
    >>| fun result ->
    match result with
    | Ok () -> Ok ()
    | Error exit_or_signal ->
      Error (Error.create "Parallel's master process did not exit cleanly" exit_or_signal
               ([%sexp_of: Unix.Exit_or_signal.error]))
;;

let spawn (type a) (type b) (type c)
    ?buffer_age_limit ?where (f : (a, b) Hub.t -> c Deferred.t) =
  Master_process.create_process ?where () >>= function
  | Error e -> failwithf "error talking to master process %s" (Error.to_string_hum e) ()
  | Ok (addr, port) ->
    let module C = Channel in
    let module T = Worker_process.To_worker in
    let module F = Worker_process.From_worker in
    C.create ?buffer_age_limit ~addr:(addr, port) ()
    (* [control_channel] is the first connection that the worker accepts.  We write on it
       the task to run, and then wait for the worker to send back the result. *)
    >>= fun (control_channel : ((a, b, c) T.t, c F.t) C.t) ->
    C.create ?buffer_age_limit ~addr:(addr, port) ()
    >>= fun data_channel ->
    let close c =
      don't_wait_for (Monitor.try_with (fun () -> C.close c) >>| ignore)
    in
    C.write control_channel (Worker_process.To_worker.Run (buffer_age_limit, f));
    C.flushed control_channel
    >>| fun () ->
    let res =
      C.read_full control_channel >>| (function
      | `Eof -> Error "Eof while reading process result"
      | `Ok r ->
        match r with
        | F.Result a -> Ok a
        | F.Exn e -> Error e)
    in
    upon res (fun _ -> close control_channel; close data_channel);
    (data_channel, res)
;;

let run ?buffer_age_limit ?where f =
  spawn ?buffer_age_limit ?where (fun _ -> f ()) >>= fun (_, res) -> res
;;

let st = lazy (Random.State.make_self_init ())
let hub ?buffer_age_limit () =
  let rec pick_port port =
    let s = Socket.create Socket.Type.tcp in
    Monitor.try_with (fun () ->
      Socket.bind s (Socket.Address.Inet.create Unix.Inet_addr.bind_any ~port))
    >>= function
    | Ok s -> return s
    | Error r ->
      match Monitor.extract_exn r with
      | Unix.Unix_error (EADDRINUSE, _, _) ->
        ignore (Monitor.try_with (fun () -> Unix.close (Socket.fd s)));
        pick_port (port + 1 + Random.State.int (Lazy.force st) 10)
      | exn -> raise exn
  in
  pick_port 10000 >>= fun s ->
  Hub.create ?buffer_age_limit (Socket.listen s)
;;

let is_worker_machine () = Core.Sys.getenv "ASYNC_PARALLEL_IS_CHILD_MACHINE" <> None

let round_robin = Master_process.round_robin
let random = Master_process.random
let random_in = Master_process.random_in
