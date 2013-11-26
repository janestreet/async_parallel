open Core.Std
module Std_unix = Unix
open Async.Std

let _p s = Printf.printf "%s: %s\n%!" (Time.to_string (Time.now ())) s
let _size a = String.length (Marshal.to_string a [Marshal.Closures])

type reified =
  { fd: Std_unix.File_descr.t;
    reader: Reader.t;
    writer: Writer.t;
  }

let next_id = ref 0
let reified = Int.Table.create ()


type ('a, 'b) t =
  { socket : Std_unix.Inet_addr.t * int;
    mutable token : Token.t;
    mutable state: [ `Unconnected
                   | `Reified of int
                   | `Reifying of Bigstring.t Queue.t * unit Deferred.t
                   | `Dead of exn ];
    buffer_age_limit:[ `At_most of Time.Span.t | `Unlimited ] option;
    mutable errors : exn Tail.t option;
  }

let socket t = t.socket

let on_error t exn =
  match t.errors with
  | None -> raise exn
  | Some errors -> Tail.extend errors exn;
;;

let reify t =
  let s = Socket.create Socket.Type.tcp in
  Socket.connect s (`Inet t.socket)
  >>| fun s ->
  let r =
    { fd = Unix.Fd.file_descr_exn (Socket.fd s);
      reader = Reader.create (Socket.fd s);
      writer = Writer.create ?buffer_age_limit:t.buffer_age_limit (Socket.fd s);
    }
  in
  Stream.iter_durably (Monitor.errors (Writer.monitor r.writer)) ~f:(on_error t);
  r
;;

let errors t =
  let errors =
    match t.errors with
    | None ->
      let errors = Tail.create () in
      t.errors <- Some errors;
      errors
    | Some errors ->  errors
  in
  Tail.collect errors
;;

let create ?buffer_age_limit ~addr () =
  return {
    socket = addr;
    token = Token.mine;
    state = `Unconnected;
    buffer_age_limit;
    errors = None;
  }
;;

let rec rereify t =
  t.token <- Token.mine;
  let q = Queue.create () in
  let r =
    reify t >>| (fun r ->
      match t.state with
      | `Unconnected -> assert false
      | `Dead _ ->
        (* Being here implies we wern't sent to another process (because the deferred
           returned by reify can never be filled if we are), so it's ok to close the
           writer. *)
        don't_wait_for
          (Monitor.try_with
             (fun () -> Writer.close r.writer) >>| ignore)
      | `Reified _ -> assert false
      | `Reifying _ ->
        let id = !next_id in
        incr next_id;
        t.state <- `Reified id;
        Hashtbl.replace reified ~key:id ~data:r;
        Queue.iter q ~f:(fun v -> write_bigstring ~can_destroy:false t v))
  in
  t.state <- `Reifying (q, r);
  r

and write_bigstring ~can_destroy t v =
  match t.state with
  | `Dead exn -> on_error t exn
  | _ ->
    if not (Token.valid t.token) then begin
      let _ = rereify t in
      write_bigstring ~can_destroy t v
    end else begin
      match t.state with
      | `Unconnected ->
        don't_wait_for (rereify t);
        (* Calling [write_bigstring] isn't an infinite loop, because [rereify] changes the
           state of [t] to [`Reifying]. *)
        write_bigstring ~can_destroy t v;
      | `Reified r ->
        begin match Hashtbl.find reified r with
        | None -> assert false
        | Some r ->
          try
            Writer.schedule_bigstring r.writer v;
            if can_destroy
            then (Writer.flushed r.writer >>> fun () -> Bigstring.unsafe_destroy v)
            else ()
          with exn -> on_error t exn
        end
      | `Reifying (q, _) -> Queue.enqueue q v
      | `Dead _ -> assert false
    end
;;

type 'a pre_packed = Bigstring.t

let pre_pack v =
  try
    Bigstring_marshal.marshal ~flags:[Marshal.Closures] v
  with e ->
    let tag = Obj.tag (Obj.repr v) in
    let size = Obj.size (Obj.repr v) in
    let subv = Obj.field (Obj.repr v) 0 in
    Printf.printf "Channel.pre_pack: tag=%d; size=%d; exn %s\n%!" tag size (Exn.to_string e);
    Printf.printf "subv: tag = %d; size = %d\n%!" (Obj.tag subv) (Obj.size subv);
    List.iter ~f:(fun (name, tag) -> Printf.printf "%s = %d\n%!" name tag) [
      "custom_tag", Obj.custom_tag;
      "lazy_tag", Obj.lazy_tag;
      "closure_tag", Obj.closure_tag;
      "object_tag", Obj.object_tag;
      "infix_tag", Obj.infix_tag;
      "forward_tag", Obj.forward_tag;
      "no_scan_tag", Obj.no_scan_tag;
      "abstract_tag", Obj.abstract_tag;
      "string_tag", Obj.string_tag;
      "double_tag", Obj.double_tag;
      "double_array_tag", Obj.double_array_tag;
      "custom_tag", Obj.custom_tag;
      "final_tag", Obj.final_tag;
      "int_tag", Obj.int_tag;
      "out_of_heap_tag", Obj.out_of_heap_tag;
      "unaligned_tag", Obj.unaligned_tag;
    ];
    raise e
;;

let write_pre_packed t v = write_bigstring ~can_destroy:false t v

let write t v =
  let bs = Bigstring_marshal.marshal ~flags:[Marshal.Closures] v in
  write_bigstring ~can_destroy:true t bs

let check_not_dead t =
  match t.state with
  | `Dead e -> raise e
  | _ -> ()
;;

let rec read_full t =
  check_not_dead t;
  if not (Token.valid t.token) then
    rereify t >>= (fun () -> read_full t)
  else begin
    match t.state with
    | `Unconnected -> rereify t >>= (fun () -> read_full t)
    | `Reified r ->
      begin match Hashtbl.find reified r with
      | None -> assert false
      | Some r -> Reader.read_marshal r.reader
      end
    | `Reifying (_, r) -> r >>= (fun () -> read_full t)
    | `Dead exn -> raise exn
  end
;;

let read t =
  read_full t >>| function
  | `Eof -> raise End_of_file
  | `Ok a -> a
;;

exception Closed with sexp

let close t =
  if Token.valid t.token then begin
    match t.state with
    | `Unconnected | `Dead _ -> return ()
    | `Reifying _ -> t.state <- `Dead Closed; return ()
    | `Reified r ->
      t.state <- `Dead Closed;
      begin match Hashtbl.find reified r with
      | None -> ()
      | Some r ->
        ignore (Monitor.try_with (fun () ->
          Writer.close r.writer
          >>= fun () ->
          Reader.close r.reader))
      end;
      Hashtbl.remove reified r;
      return ()
  end else
    return ()
;;

let rec flushed t =
  check_not_dead t;
  if not (Token.valid t.token) then Deferred.unit
  else begin
    match t.state with
    | `Unconnected -> Deferred.unit
    | `Dead e -> raise e
    | `Reifying (_, r) -> r >>= (fun () -> flushed t)
    | `Reified r ->
      begin match Hashtbl.find reified r with
      | None -> assert false
      | Some r -> Writer.flushed r.writer
      end
  end
