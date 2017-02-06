open Core
open Async
open Import

module Client_id : sig
  type t [@@deriving sexp]

  include Comparable with type t := t
  include Hashable with type t := t

  val zero : t
  val succ : t -> t
end = Int

type con = {
  reader: Reader.t;
  writer: Writer.t;
}

type 'a update =
  [ `Connect of Client_id.t
  | `Disconnect of Client_id.t * string
  | `Data of (Client_id.t * 'a)
  ]

type ('a, 'b) t =
  { mutable next_client: Client_id.t;
    mutable listening: bool;
    shutdown: unit Ivar.t;
    is_shutdown: unit Ivar.t;
    pipe_r: 'a update Pipe.Reader.t;
    pipe_w: 'a update Pipe.Writer.t;
    clients: (Client_id.t, con) Hashtbl.t;
    socket: ([`Passive], Socket.Address.Inet.t) Socket.t;
    addr : Unix.Inet_addr.t * int;
    token: Token.t;
    buffer_age_limit: Writer.buffer_age_limit option;
  }

exception Hubs_are_not_portable_between_processes

let close t =
  if not t.listening then
    Monitor.try_with (fun () -> Unix.close (Socket.fd t.socket)) >>| ignore
  else begin
    Ivar.fill_if_empty t.shutdown ();
    Ivar.read t.is_shutdown
  end

let create ?buffer_age_limit socket =
  let ip = Lazy.force my_ip in
  let `Inet (_, port) = Socket.getsockname socket in
  let (pipe_r, pipe_w) = Pipe.create () in
  return
    { next_client = Client_id.zero;
      listening = false;
      shutdown = Ivar.create ();
      is_shutdown = Ivar.create ();
      pipe_r;
      pipe_w;
      clients = Hashtbl.Poly.create () ~size:1;
      socket;
      addr = (ip, port);
      buffer_age_limit;
      token = Token.mine; }
;;

let handle_client t ~close ~conn ~id ~stop =
  let read () = try_with (fun () -> Reader.read_marshal conn.reader) in
  let rec loop () =
    let res =
      choose
        [ choice stop (fun () -> `Stop);
          choice (read ()) (fun x -> `Read x);
        ]
    in
    res >>> function
      | `Stop -> ()
      | `Read x ->
        match x with
        | Error e -> close (Exn.to_string e)
        | Ok x ->
          match x with
          | `Eof -> close "saw EOF while reading"
          | `Ok a ->
            Pipe.write t.pipe_w (`Data (id, a))
            >>> fun () ->
            if Option.is_none (Deferred.peek stop) then loop ()
  in
  loop ()
;;

let listener t =
  let shutdown = Ivar.read t.shutdown in
  let rec loop () =
    try_with (fun () -> Socket.accept_interruptible ~interrupt:shutdown t.socket)
    >>> function
    | Ok `Interrupted -> begin
      let cl = Hashtbl.data t.clients in
      Deferred.List.iter ~how:`Parallel cl ~f:(fun {reader=_; writer} ->
        Monitor.try_with (fun () -> Writer.close writer) >>| ignore)
      >>> fun () ->
      Monitor.try_with (fun () -> Unix.close (Socket.fd t.socket))
      >>> fun _ ->
      Ivar.fill_if_empty t.is_shutdown ()
    end
    | Error e ->
      Monitor.send_exn (Monitor.current ()) e;
      Clock.after (sec 0.5) >>> loop
    | Ok `Socket_closed -> ()
    | Ok (`Ok (sock, _)) ->
      let fd = Socket.fd sock in
      let id =
        let id = t.next_client in
        t.next_client <- Client_id.succ id;
        id
      in
      let conn = { writer = Writer.create ?buffer_age_limit:t.buffer_age_limit fd;
                   reader = Reader.create fd } in
      let closed = Ivar.create () in
      let close =
        let error = ref "" in
        let close =
          lazy (Pipe.write t.pipe_w (`Disconnect (id, !error))
                >>> fun () ->
                don't_wait_for
                  (Monitor.try_with (fun () ->
                    Writer.close conn.writer) >>| ignore);
                Hashtbl.remove t.clients id;
                Ivar.fill closed ())
        in
        (fun e -> error := e; Lazy.force close)
      in
      Stream.iter (Monitor.detach_and_get_error_stream (Writer.monitor conn.writer)) ~f:(fun e ->
        let s = Exn.to_string e in
        Core.Printf.printf "%s hub writer error %s\n%!" (Pid.to_string (Unix.getpid ())) s;
        close s);
      Hashtbl.add_exn t.clients ~key:id ~data:conn;
      handle_client t ~close ~conn ~id ~stop:(Ivar.read closed);
      Pipe.write t.pipe_w (`Connect id)
      >>> loop
  in
  loop ()
;;

let ensure_valid t =
  if not (Token.valid t.token) then raise Hubs_are_not_portable_between_processes
;;

let listen t =
  ensure_valid t;
  if t.listening then failwith "already listening";
  t.listening <- true;
  listener t;
  t.pipe_r
;;

let listen_simple t =
  Pipe.filter_map (listen t) ~f:(function
    | `Connect _ | `Disconnect _ -> None
    | `Data (c, a) -> Some (c, a))
;;

let send t id a =
  ensure_valid t;
  match Hashtbl.find t.clients id with
  | None -> ()
  | Some conn ->
    Writer.write_marshal conn.writer ~flags:[Marshal.Closures] a
;;

let send_to_all t a =
  ensure_valid t;
  let s = Marshal.to_string a [Marshal.Closures] in
  Hashtbl.iteri t.clients ~f:(fun ~key:_ ~data:conn -> Writer.write conn.writer s);
;;

let flushed t =
  Deferred.all_unit
    (Hashtbl.fold t.clients ~init:[] ~f:(fun ~key:_ ~data:conn acc ->
      Writer.flushed conn.writer :: acc))
;;

let open_channel t = Channel.create ~addr:t.addr ()
let socket t = t.addr
let clients t = Hashtbl.keys t.clients
