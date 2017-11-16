(* The master process cannot use async, because async would start threads.  We also need
   to throw away any async state that might have been there when we were forked. *)

open Core
open Import

module U = Unix

module File_descr = U.File_descr

module Request = struct
  type t =
  | Create_worker_process
  | Shutdown
  | Heartbeat
end

module Update = struct
  type t =
  | Create_worker_process_response of (U.Inet_addr.t * int) Or_error.t
  | Heartbeat
end

exception Cannot_find_available_port

let syscall th =
  let rec loop () =
    try th ()
    with U.Unix_error (EINTR, _, _) -> loop ()
  in loop ()

let st = lazy (Random.State.make_self_init ())
let listener () =
  let rec loop port =
    let s = U.socket ~domain:U.PF_INET ~kind:U.SOCK_STREAM ~protocol:0 in
    try
      U.bind s ~addr:(U.ADDR_INET (U.Inet_addr.bind_any, port));
      U.listen ~backlog:500 s;
      U.set_nonblock s;
      U.setsockopt s U.SO_REUSEADDR true;
      s, (Lazy.force my_ip, port)
    with
    | U.Unix_error (EADDRINUSE, _, _) ->
      syscall (fun () -> U.close s);
      let port = port + 1 + Random.State.int (Lazy.force st) 10 in
      if port < 30000 then loop port
      else raise Cannot_find_available_port
    | exn -> raise exn
  in
  loop 10000
;;

let really_io
    (th : ?pos:int -> ?len:int -> File_descr.t -> buf:'a -> int)
    fd (buf : 'a) =
  let rec loop ~pos ~len =
    match syscall (fun () -> th fd ~buf ~pos ~len) with
    | 0 -> raise End_of_file
    | finished ->
      if finished < len then
        loop ~pos:(pos + finished) ~len:(len - finished)
  in
  loop
;;

let write c update =
  let s = Marshal.to_string update [] in
  really_io (U.single_write_substring ~restart:false) c s ~len:(String.length s) ~pos:0
;;

let read =
  assert (Marshal.header_size < 4096);
  let buf = ref (Bytes.create 4096) in
  fun c ->
    really_io (U.read ~restart:false) c !buf ~pos:0 ~len:Marshal.header_size;
    let len = Marshal.data_size !buf 0 in
    if len + Marshal.header_size > Bytes.length !buf then begin
      let new_buf = Bytes.create (len + Marshal.header_size) in
      Bytes.blit
        ~src:!buf ~dst:new_buf ~src_pos:0
        ~dst_pos:0 ~len:Marshal.header_size;
      buf := new_buf
    end;
    really_io (U.read ~restart:false) c !buf ~pos:Marshal.header_size ~len;
    Marshal.from_bytes !buf 0
;;

module Signals = struct
  module S = Signal

  let existing = ref []

  let handle ~onexit ~onchld =
    let exit_sigs = [S.hup; S.int; S.quit; S.term] in
    List.iter exit_sigs ~f:(fun s ->
      let existing_behavior = S.Expert.signal s (`Handle onexit) in
      (* allow the user to ignore some (or all) exit signals. *)
      if existing_behavior = `Ignore then S.ignore s
      else existing := (s, existing_behavior) :: !existing);
    let existing_chld = S.Expert.signal S.chld (`Handle onchld) in
    existing := (S.chld, existing_chld) :: !existing;
    let existing_pipe = S.Expert.signal S.pipe `Ignore in
    existing := (S.pipe, existing_pipe) :: !existing;
  ;;

  let restore () = List.iter !existing ~f:(fun (s, b) -> S.Expert.set s b)
end

(* This table contains all machines master process addresses *)
let machines = lazy (String.Table.create ())

(* Only the master machine has this, it is a list of Unix.Process_info.t for each child
   machine the master spawned. *)
let child_machines = ref []

(* This is the name of local machine *)
let local_name = ref None

(* This is the name of the master machine (as found in the machines table) *)
let master_name = ref None

let create_worker_process () =
  (* Create a listening socket for the child so it can communicate with other
     processes. We will communicate the address back to the creator. *)
  let (s, addr) = listener () in
  match syscall (fun () -> U.fork ()) with
  | `In_the_parent pid ->
    if debug then dbp (sprintf "created worker process with pid %d" (Pid.to_int pid));
    syscall (fun () -> U.close s);
    (pid, addr)
  | `In_the_child ->
    Signals.restore ();
    (* We're in the child, so it's OK to use Async. *)
    let open Async in
    let control_socket =
      Socket.of_fd
        (Fd.create (Fd.Kind.Socket `Passive) s
           (Info.of_string
              (sprintf "<child %s control socket>"
                 (Pid.to_string (U.getpid ())))))
        Socket.Type.tcp
    in
    never_returns (Worker_process.run ~control_socket)
;;

let talk_machine ip port talk =
  let s = U.socket ~domain:U.PF_INET ~kind:U.SOCK_STREAM ~protocol:0 in
  let close () = try syscall (fun () -> U.close s) with _ -> () in
  try
    U.setsockopt s U.TCP_NODELAY true;
    let prev = Signal.Expert.signal Signal.alrm (`Handle (fun _ -> close ())) in
    ignore (U.alarm 60);
    syscall (fun () -> U.connect s ~addr:(U.ADDR_INET (ip, port)));
    let res = talk s in
    ignore (U.alarm 0);
    Signal.Expert.set Signal.alrm prev;
    close ();
    res
  with exn -> close (); Error exn

let ping_master_machine () =
  let last = ref (Time.now ()) in
  fun () ->
    match !master_name with
    | None -> Ok ()
    | Some name ->
      if Time.diff (Time.now ()) !last <= sec 60. then Ok ()
      else begin
        let (ip, port) = Hashtbl.find_exn (Lazy.force machines) name in
        talk_machine ip port (fun s ->
          write s Request.Heartbeat;
          match (read s : Update.t) with
          | Update.Heartbeat -> last := Time.now (); Ok ()
          | Update.Create_worker_process_response _ -> assert false)
      end

let transfer_to close in_fd out_fd =
  try
    let buf = Bytes.make 512 '\000' in
    let len = syscall (fun () -> U.read in_fd ~buf ~len:512 ~pos:0) in
    if len = 0 then close ()
    else really_io (U.single_write ~restart:false) out_fd buf ~len ~pos:0
  with _ -> close ()

(* [run] is called exactly once, in the master process, when it is first starting. *)
let run listener : never_returns =
  (* The main process may have already done some async stuff that created the async
     scheduler.  So, we reset async at the start of the master.  This puts async in
     a pristine state at the start of each worker that the master forks. *)
  Async.Scheduler.reset_in_forked_process ();
  (* We put a pipe (that we've created) into every call to select. This allows us to wake
     up select whenever we want by writing to the pipe. This is the buffer that holds the
     wakeup events. It's 50 bytes long, because we write a wakeup event for every time we
     get sigchld, and when we read we want to get all the wakeup events at once. *)
  (* These settings will cause us to shrink the heap to nearly the actual live data size
     when we compact. The smaller the heap the faster fork runs *)
  let wakeup_len = 50 in
  let wakeup_buf = Bytes.create wakeup_len in
  let (wakeup_r, wakeup_w) = U.pipe () in
  let select_interrupted = ref false in
  let children = Pid.Hash_set.create () in
  let clients = U.File_descr.Hash_set.create () in
  let ping_master_machine = ping_master_machine () in
  let child_machine_stdout = U.File_descr.Hash_set.create () in
  let child_machine_stderr = U.File_descr.Hash_set.create () in
  List.iter !child_machines ~f:(fun p ->
    Hash_set.add child_machine_stdout p.U.Process_info.stdout;
    Hash_set.add child_machine_stderr p.U.Process_info.stderr);
  let end_the_world exit_status =
    if debug then dbp "end the world called";
    Hash_set.iter children ~f:(fun pid -> ignore (Signal.send_i Signal.term (`Pid pid)));
    Time.pause (sec 1.0);
    Hash_set.iter children ~f:(fun pid -> ignore (Signal.send_i Signal.kill (`Pid pid)));
    begin match !master_name with
    | Some _ -> () (* We're not the master *)
    | None -> (* We should tell all the other machines to shutdown *)
      if debug then dbp "we're the master, so sending shutdown to everyone";
      Hashtbl.iteri (Lazy.force machines) ~f:(fun ~key:_ ~data:(ip, port) ->
        try
          Result.ok_exn (talk_machine ip port (fun s -> write s Request.Shutdown; Ok ()))
        with _ -> ())
    end;
    Pervasives.exit exit_status
  in
  Signals.handle
    ~onexit:(fun _ ->
      if debug then dbp "received exit signal";
      end_the_world 1)
    ~onchld:(fun _ ->
      if not !select_interrupted then begin
        select_interrupted := true;
        ignore (syscall (fun () -> U.write wakeup_w ~buf:wakeup_buf ~pos:0 ~len:1))
      end);
  try
    while true do
      let { U.Select_fds.read = read_fds; write=_; except=_ } =
        try
          let fds =
            wakeup_r :: listener
            :: Hash_set.to_list child_machine_stdout
            @ Hash_set.to_list child_machine_stderr
            @ Hash_set.to_list clients
          in
          syscall (fun () ->
            U.select ~read:fds ~write:[] ~except:[]
              ~timeout:(`After Time_ns.Span.second) ())
        with
        | U.Unix_error (EBADF, _, _) ->
          Hash_set.filter_inplace clients ~f:(fun fd ->
          try ignore (syscall (fun () -> U.fstat fd)); true
          with _ ->
            (try syscall (fun () -> U.close fd) with _ -> ());
            false);
          U.Select_fds.empty
      in
      (* if we become a child of init then we should die *)
      if U.getppid () = Some Pid.init && !master_name = None then end_the_world 0;
      let rec reap () =
        match (try syscall (fun () -> U.wait_nohang `Any) with _ -> None) with
        | None -> ()
        | Some (pid, e) ->
          if debug then dbp (sprintf "pid %d died with %s\n%!"
            (Pid.to_int pid) (Sexp.to_string (U.Exit_or_signal.sexp_of_t e)));
          Hash_set.remove children pid;
          reap ()
      in
      if !select_interrupted then reap ();
      (* If we can no longer contact the master machine, then we should
         shutdown. *)
      begin match ping_master_machine () with
      | Ok () -> () (* We successfully pinged the master server *)
      | Error _ ->
        if debug then dbp "failed to ping master machine, ending the world";
        end_the_world 0
      end;
      List.iter read_fds ~f:(fun fd ->
        if fd = wakeup_r then begin
          ignore (syscall (fun () ->
            U.read wakeup_r ~buf:wakeup_buf ~pos:0 ~len:wakeup_len));
          select_interrupted := false;
        end else if fd = listener then begin
          let rec accept () =
            let res =
              try Some (U.accept listener) with
              | U.Unix_error ((EWOULDBLOCK | EAGAIN | ECONNABORTED | EINTR), _, _)
                -> None
            in
            match res with
            | None -> ()
            | Some (fd, _addr) ->
              Hash_set.add clients fd;
              accept ()
          in accept ();
        end else if Hash_set.mem child_machine_stdout fd then begin
          transfer_to (fun () ->
            Hash_set.remove child_machine_stdout fd;
            try U.close fd with _ -> ()) fd U.stdout
        end else if Hash_set.mem child_machine_stderr fd then begin
          transfer_to (fun () ->
            Hash_set.remove child_machine_stderr fd;
            try U.close fd with _ -> ()) fd U.stderr
        end else begin
          let v = try `Ok (read fd : Request.t) with _ -> `Read_error in
          match v with
          | `Read_error ->
            Hash_set.remove clients fd;
            syscall (fun () -> U.close fd)
          | `Ok Request.Shutdown ->
            if debug then dbp "master process shutdown requested";
            end_the_world 0;
          | `Ok Request.Create_worker_process ->
            begin match Or_error.try_with create_worker_process with
            | Error _ as r -> write fd (Update.Create_worker_process_response r)
            | Ok (pid, addr) ->
              Hash_set.add children pid;
              write fd (Update.Create_worker_process_response (Ok addr))
            end
          | `Ok Request.Heartbeat -> write fd Update.Heartbeat
        end)
    done;
    end_the_world 2
  with exn ->
    if debug then
      Debug.log_string
        (sprintf "master died with unhandled exception %s" (Exn.to_string exn));
    if debug then
      dbp (sprintf "ending the world on unhandled exception %s" (Exn.to_string exn));
    end_the_world 3
;;

exception Error_initializing_worker_machine of string * exn [@@deriving sexp]

module Worker_machines = struct
  module To_worker_machine = struct
    type t = {
      master_name: string;
      machines: (U.Inet_addr.Blocking_sexp.t * int) String.Table.t;
    } [@@deriving sexp]
  end

  module From_worker_machine = struct
    type t = (U.Inet_addr.Blocking_sexp.t * int) [@@deriving sexp]
  end

  (* We're a worker machine, not the master. *)
  let as_worker_machine worker_name =
    local_name := Some worker_name;
    (* Take out the trash *)
    let our_binary = U.readlink (sprintf "/proc/%d/exe" (Pid.to_int (U.getpid ()))) in
    U.unlink our_binary;
    U.rmdir (Filename.dirname our_binary);
    let master_cwd = Option.value_exn (Sys.getenv "ASYNC_PARALLEL_MASTER_CWD") in
    if Sys.file_exists ~follow_symlinks:true master_cwd = `Yes
       && Sys.is_directory ~follow_symlinks:true master_cwd = `Yes
       && U.access master_cwd [`Exec] = Ok ()
    then U.chdir master_cwd
    else U.chdir "/";
    let (listening_socket, addr) = listener () in
    (* Tell the master machine our address. *)
    printf "%s\n%!" (Sexp.to_string_mach (From_worker_machine.sexp_of_t addr));
    (* Accept one connection so we can read everyone else's address from the master
       machine *)
    let rec accept () =
      let res =
        try Some (U.accept listening_socket) with
        | U.Unix_error ((EWOULDBLOCK | EAGAIN | ECONNABORTED | EINTR), _, _) ->
          None
      in
      match res with
      | None -> Time.pause (sec 0.1); accept ()
      | Some (fd, _) -> fd
    in
    let s = accept () in
    let ic = U.in_channel_of_descr s in
    let l = Option.value_exn (In_channel.input_line ic) in
    let m = To_worker_machine.t_of_sexp (Sexp.of_string l) in
    In_channel.close ic;
    let tbl = Lazy.force machines in
    Hashtbl.clear tbl;
    Hashtbl.iteri m.To_worker_machine.machines ~f:(fun ~key ~data ->
      Hashtbl.set tbl ~key ~data);
    master_name := Some (m.To_worker_machine.master_name);
    never_returns (run listening_socket)
  ;;

  (* All hail the great designers of the unix shell and operating system. *)
  let cmd bin_name cwd local_name =
    let async_config_set_var =
      let var = Async.Async_config.environment_variable in
      match Sys.getenv var with
      | None -> ""
      | Some value -> sprintf "%s=%S" var value
    in
    sprintf "(
US=$(mktemp -d)
EXE=${US}/%s
# the master machine is going to send then binary across stdin, and
# then close it, which will cause cat to exit, and the binary to run.
# we don't need stdin anyway, so it's fine if it gets closed.
cat >\"$EXE\"
chmod 700 \"$EXE\"
%s \\
ASYNC_PARALLEL_MASTER_CWD=\"%s\" \\
ASYNC_PARALLEL_IS_CHILD_MACHINE=\"%s\" \\
\"$EXE\" </dev/null || rm -rf $US
)" bin_name async_config_set_var cwd local_name

  (* This prevents 'The authenticity of host can't be established.... Are you sure you
     want to continue connecting (yes/no)? ' messages *)
  let ssh_options = ["-o"; "StrictHostKeyChecking=no"]

  (* We assume our own binary is not replaced during initialization. Sadly this is the
     best we can do, as unix provides no way to access the file of an unlinked program
     (proc/exe is just a symlink). *)
  let init cluster =
    let our_cwd = Sys.getcwd () in
    let our_binary = U.readlink (sprintf "/proc/%d/exe" (Pid.to_int (U.getpid ()))) in
    let us = In_channel.read_all our_binary in
    let tbl = Lazy.force machines in
    let addrs =
      List.map cluster.Cluster.worker_machines ~f:(fun machine ->
        let module P = U.Process_info in
        let p =
          U.create_process ~prog:"ssh"
            ~args:(ssh_options @ [machine; cmd (Filename.basename our_binary) our_cwd machine])
        in
        try
          really_io (U.single_write_substring ~restart:false) p.P.stdin us
            ~len:(String.length us) ~pos:0;
          U.close p.P.stdin;
          let ic = U.in_channel_of_descr p.P.stdout in
          let (addr, port) =
            (* The program might say other stuff during startup on stdout, just skip until
               we find our sexp. *)
            let rec loop () =
              let line = Option.value_exn (In_channel.input_line ic) in
              try From_worker_machine.t_of_sexp (Sexp.of_string line)
              with _ -> loop ()
            in loop ()
          in
          child_machines := p :: !child_machines;
          Hashtbl.set tbl ~key:machine ~data:(addr, port);
          (addr, port)
        with e ->
          (try ignore (Signal.send Signal.kill (`Pid p.P.pid) : [`No_such_process | `Ok])
           with _ -> ());
          (try U.close p.P.stdout with _ -> ());
          (try U.close p.P.stderr with _ -> ());
          (try ignore (U.wait_nohang (`Pid p.P.pid) : (Pid.t * U.Exit_or_signal.t) option)
           with _ -> ());
          raise (Error_initializing_worker_machine (machine, e)))
    in
    let m =
      { To_worker_machine.
        machines = tbl;
        master_name = cluster.Cluster.master_machine }
    in
    let sexp = Sexp.to_string_mach (To_worker_machine.sexp_of_t m) in
    List.iter addrs ~f:(fun (ip, port) ->
      Result.ok_exn
        (talk_machine ip port (fun s ->
          let oc = U.out_channel_of_descr s in
          Out_channel.output_lines oc [sexp];
          Out_channel.flush oc;
          Ok ())))
end

let init ?cluster ?(close_stdout_and_stderr = false) () =
  let worker_name = Sys.getenv "ASYNC_PARALLEL_IS_CHILD_MACHINE" in
  if worker_name <> None then
    Worker_machines.as_worker_machine (Option.value_exn worker_name)
  else if Hashtbl.length (Lazy.force machines) > 0 then
    failwith "Master process already initialized"
  else begin
    let (listening_socket, addr) = listener () in
    begin match cluster with
    | None ->
      local_name := Some "local";
      Hashtbl.set (Lazy.force machines) ~key:(Option.value_exn !local_name)
        ~data:addr;
    | Some c ->
      local_name := Some c.Cluster.master_machine;
      Hashtbl.set (Lazy.force machines) ~key:(Option.value_exn !local_name)
        ~data:addr;
      try Worker_machines.init c
      with e ->
        (* Set up our state so it can be initialized again *)
        (try U.close listening_socket with _ -> ());
        local_name := None;
        Hashtbl.clear (Lazy.force machines);
        raise e
    end;
    match U.fork () with
    | `In_the_child ->
      (* The master process *)
       begin
         if close_stdout_and_stderr then
           (Unix.(close stdout); Unix.(close stderr))
       end;
       never_returns (run listening_socket);
    | `In_the_parent master_pid ->
      (* The main process *)
      if debug then
        dbp (sprintf "created master process with pid %s" (Pid.to_string master_pid));
      syscall (fun () -> U.close listening_socket);
      master_pid;
  end
;;

(* Code below here does not run in the master process, and so can use async. *)
open Async

type host = {
  q: (Update.t, string) Result.t Ivar.t Queue.t;
  mutable listening: bool;
  mutable conn: [ `Connecting of unit Deferred.t
                | `Connected of (Reader.t * Writer.t)
                | `Not_connected ];
}

let create () = {q = Queue.create (); listening = false; conn = `Not_connected}

let hosts : host String.Table.t = String.Table.create ()

let kill ?(e = Error "killed") h =
  begin match h.conn with
  | `Not_connected -> assert false
  | `Connecting _ -> assert false
  | `Connected (r, w) ->
    ignore (Monitor.try_with (fun () -> Writer.close w));
    ignore (Monitor.try_with (fun () -> Reader.close r))
  end;
  h.listening <- false;
  h.conn <- `Not_connected;
  Queue.iter h.q ~f:(fun i -> Ivar.fill i e);
  Queue.clear h.q
;;

let listen h =
  if h.listening then ()
  else begin
    let rec loop () =
      match h.conn with
      | `Connecting _ -> assert false
      | `Not_connected -> assert false
      | `Connected (r, _) ->
        Monitor.try_with (fun () ->
          (Reader.read_marshal r : Update.t Reader.Read_result.t Deferred.t))
        >>> function
        | Error e -> kill ~e:(Error (Exn.to_string e)) h
        | Ok `Eof -> kill ~e:(Error "Eof") h
        | Ok (`Ok a) ->
          Ivar.fill (Queue.dequeue_exn h.q) (Ok a);
          (* we don't keep a persistent connection to the master process,
             since many processes on many machines may be talking to it,
             and we don't want to run it out of file descriptors. *)
          if Queue.is_empty h.q then kill h else loop ()
    in
    h.listening <- true;
    loop ()
  end
;;

let rec connected_host host f =
  let machines = Lazy.force machines in
  let (addr, port) = Hashtbl.find_exn machines host in
  let connect h =
    let s = Socket.create Socket.Type.tcp in
    let rw =
      socket_connect_inet s (addr, port) >>| fun s ->
      let w = Writer.create (Socket.fd s) in
      let r = Reader.create (Socket.fd s) in
      (r, w)
    in
    h.conn <- `Connecting (rw >>| fun (_r, _w) -> ());
    rw >>| fun (r, w) ->
    h.conn <- `Connected (r, w);
    (h, w)
  in
  match Hashtbl.find_or_add hosts host ~default:create with
  | {conn = `Connecting rw; _} -> (rw >>= fun () -> connected_host host f)
  | {conn = `Not_connected; _} as h -> (connect h >>= fun _ -> connected_host host f)
  | {conn = `Connected (_, w); _} as h -> f (h, w)
;;

let choose_machine where =
  let tbl = Lazy.force machines in
  if Hashtbl.length tbl = 0 then failwith "Parallel.init not called";
  match where with
  | `Local -> Option.value_exn !local_name
  | `On host ->
    if Hashtbl.mem tbl host then host
    else failwithf "unknown host %s" host ()
  | `F f ->
    let host = f () in
    if Hashtbl.mem tbl host then host
    else failwithf "unknown host %s" host ()
;;

let create_process ?(where = `Local) () =
  connected_host (choose_machine where) (fun (h, w) ->
    Deferred.create (fun i ->
      Queue.enqueue h.q i;
      Writer.write_marshal w ~flags:[] Request.Create_worker_process;
      listen h))
  >>| function
  | Error e -> Error (Error.of_string e)
  | Ok (Update.Create_worker_process_response addr) -> addr
  | Ok Update.Heartbeat -> assert false
;;

let shutdown () =
  if debug then dbp "Master_process.shutdown called";
  connected_host (Option.value_exn !local_name) (fun (h, w) ->
    Writer.write_marshal w ~flags:[] Request.Shutdown;
    Writer.flushed w
    >>| fun () ->
    kill h;
    Hashtbl.clear (Lazy.force machines);
    child_machines := [];
    local_name := None;
    master_name := None)
;;


(* Process distribution methods *)

let round_robin =
  let i = ref 0 in
  `F (fun () ->
    let tbl = Lazy.force machines in
    let a = Array.of_list (Hashtbl.keys tbl) in
    let current = !i in
    incr i;
    if !i >= Array.length a then i := 0;
    a.(current))

let random =
  `F (fun () ->
    let tbl = Lazy.force machines in
    let st = Lazy.force st in
    let a = Array.of_list (Hashtbl.keys tbl) in
    a.(Random.State.int st (Array.length a)))

let random_in set =
  List.iter set ~f:(fun s ->
    if not (Hashtbl.mem (Lazy.force machines) s) then
      failwithf "unknown host in host set %s" s ());
  `F (fun () ->
    let st = Lazy.force st in
    let a = Array.of_list set in
    a.(Random.State.int st (Array.length a)))
