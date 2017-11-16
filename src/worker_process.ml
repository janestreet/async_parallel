open Core
open Async
open Import

module To_worker = struct
  type ('a, 'b, 'c) t =
  | Run of Writer.buffer_age_limit option * (('a, 'b) Hub.t -> 'c Deferred.t)
end

module From_worker = struct
  type 'a t =
  | Result of 'a
  | Exn of string
end

let go (type a) (type b) (type c) ~control_socket =
  if debug then dbp "worker process starting";
  let monitor = Monitor.create () in
  Stream.iter (Monitor.detach_and_get_error_stream monitor) ~f:(fun e ->
    if debug then
      dbp (sprintf "Worker_process: unhandled exception %s" (Exn.to_string e));
    Shutdown.shutdown 1);
  within ~monitor (fun () ->
    if debug then dbp "Worker_process.run";
    Socket.accept control_socket >>> function
    | `Socket_closed -> ()
    | `Ok (client, _) ->
      let fd = Socket.fd client in
      let owner_reader = Reader.create fd in
      let owner_writer = Writer.create fd in
      Reader.read_marshal_raw owner_reader
      >>> function
      | `Eof ->
        if debug then dbp "Worker_process: Eof reading job";
        Shutdown.shutdown 2
      | `Ok x ->
        if debug then dbp "worker process read request";
        match (Marshal.from_bytes x 0 : (a, b, c) To_worker.t) with
        | To_worker.Run (buffer_age_limit, f) ->
          if debug then dbp "got run request, creating hub";
          Hub.create ?buffer_age_limit control_socket
          >>> fun hub ->
          if debug then dbp "running f";
          (Monitor.try_with (fun () -> f hub)
           >>| (function
           | Error e ->
             if debug then dbp "result is exn";
             From_worker.Exn (Exn.to_string e)
           | Ok r ->
             if debug then dbp "result is success";
             From_worker.Result r))
          >>> fun res ->
          if debug then dbp "writing result";
          Writer.write_marshal ~flags:[Marshal.Closures] owner_writer
            (res : c From_worker.t);
          Writer.flushed owner_writer
          >>> fun _ ->
          if debug then dbp "wrote result";
          shutdown 0)

let run ~control_socket =
  go ~control_socket;
  Scheduler.go ();
;;
