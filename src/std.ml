(** DEPRECATION WARNING: Async_parallel has been deprecated in favor of Rpc_parallel. *)


(** [Parallel] is a library for running tasks in other processes on a cluster of machines.
    At its simplest, it exposes the [Parallel.run] function:

    |  val run : ?where:[`Local | `On of string | `Random | `Random_on of string list]
         -> (unit -> 'a Deferred.t) -> ('a, string) Result.t Deferred.t

    where [run f] creates another process on the machine specified by [where] whose sole
    job is to compute [f ()].  The process that calls [f] will receive the result of [f
    ()] when it finishes.  Note that [f] itself could call [run], thus allowing an
    arbitrarily nested processes across arbitrary machines in the cluster.

    In order to use, [Parallel.run], for technical reasons, one must first call
    [Parallel.init], which must be called before any threads are created and before
    Async's scheduler is started.

    Parallel's "hubs" and "channels" support typed bidirectional communication of streams
    of data between process.  One process creates a hub and listens to it.  Any other
    processes can then open a channel to the hub, and write values on the channel that
    will be received by the process listening to the hub.  Similarly, the hub process can
    send values via a channel to the process that opened the channel.

    Moreover, channels may be passed between processes, either implicitly by being
    captured in a closure, or explicitly over another channel.

    Implementation overview
    =======================
    There are three kinds of processes involved in a program the uses Parallel:

    - the main process
    - the master process
    - worker processes

    Parallel dynamically creates a worker process to service each call to [run].

    The OS process tree looks like:

    | main
    |    master
    |      worker1
    |      ...
    |      workerN

    As far as the OS is concerned, all workers are children of the master.  However, from
    the perspective of Parallel, the topology is more structured.  Each worker process is
    created on behalf of its "owner" process, which is either the main process or another
    worker process.  One can think of the main and worker processes as arranged in a tree
    different than the OS tree, in which there is an edge from each process to its owner
    (the main process has no owner).

    Parallel uses OCaml's [Marshal] library to serialize OCaml values to and from strings
    so that they can be sent over unix sockets between processes.  For example, the [f]
    supplied to [run] is marshalled and sent from the process that calls [run] to the
    worker process that will run [f]. Most, but not all values can be marshaled. Examples
    of values that can't be marshaled include C allocated abstract tagged values, custom
    blocks with no serialize/deserialize method.

    The main process and all worker processes have a socket connected to the master
    process.  The master process's sole job is to service requests that are sent to
    these sockets, which can ask it to:

    - create a new "worker process", via [create_process]

    As the master process receives requests, it does what each request asks, and then
    sends a response back via the socket to the client that made the request.

    Each worker process has a socket connected to its owner process.  This socket
    initially receives the [f] that the worker is to run, and is ultimately used to send
    the result back from the worker to the owner.

    Here are the steps involved in implementing [run f].  There are three processes
    involved.

    - R = the process calling [run]
    - M = the master process
    - W = the worker process running the task

    The steps are:

    1. R asks M to create W
    2. M forks W
    3. M tells R about W
    4. R sends [f] to W to run
    5. W runs [f]
    6. W sends the result of [f] to R
    7. M notices W has exited, and cleans up

    When there are multiple machines in a cluster, each machine has a master process,
    and all the workers know about all master processes. When a worker wants to run on
    machine M, it looks up the address of that machine's master process in its table
    before performing step 1, everything after that is exactly the same as the example.

    Notes:

    Channel Passing
    ---------------

    When a channel is passed from one process to another, the open socket is not actually
    passed. The API makes this pretty transparent, any api call will reconnect the
    channel, but it is useful to be aware of what is really going on as if you aren't
    aware you may create a race condition. For example, if I spawn a worker connected to a
    hub I have, and then I immediately send something, it may or may not arrive, because
    the worker may not have time to connect and receive it. A better strategy is to wait
    for the worker to say hello, and then send the data. This also means that you might
    have created only one channel from a given hub, but you can end up with as many
    connections (client ids) as workers who got hold of that channel. You can address them
    all individually, or you can always use [send_to_all] if you really want to model a
    hub as a kind of shared bus.

    Stdout and Stderr
    -----------------

    Care has been taken to make printf style debugging work transparently with parallel,
    even when run on a multiple machine cluster, stdout and stderr will be forwarded back
    to the master machine. This can cause some interleaving if you print a lot of
    messages, but generally works reasonably well (and we read and write in big chunks, so
    most of the interleaving won't be interline).

    Some things to avoid marshaling
    -------------------------------

    Monitor.t, Pcre.regexp, Writer.t, Reader.t, and similar kinds of objects shouldn't be
    depended upon to marshal correctly. Pcre.regexp is just right out, it definitely won't
    work. Monitor.t, Writer.t, and Reader.t, because of their complex nature, generally
    tow the entire async scheduler along with them, and because of that they will fail if
    any job on the scheduler queue has a custom object (e.g. regexp, or other C object)
    that can't be marshaled. You also can't marshal functions you've dynamically loaded
    (e.g. with ocaml plugin).

    Processes don't share memory
    ----------------------------

    The library can make it look very transparent to create and use other processes, but
    please remember these can literally be on some other machine maybe halfway round the
    earth. Global variables you set in one worker process have no effect whatsoever on
    other worker processes. I've personally come to believe that this is good, it results
    in better designed, more scalable systems.

    Big shared things
    -----------------

    Because of the way parallel works, with the master process an image of a very early
    state of one's program and workers forked from the master, it is usually not possible
    to share big static things in the way one might do in C using fork. Moreover, it isn't
    necessarily a win as you might think, if you know about how unix only copies pages
    on write when a process forks, you know that it should be a win. But the garbage
    collector ruins that completely, because as it scans it will write to EVERY page,
    causing a copy on write fault to copy the page, so you'll end up with a non shared
    copy of that big static thing in every process anyway. The best you can probably do is
    have one process own it and expose it with a query interface. Moreover, if you're
    running on multiple machines that IS the best you can do, so may as well get used to
    it.

    Why Not Just Fork!?
    -------------------

    The unix savvy among you may ask, what the heck are you doing with master processes
    and closure passing, just fork! Oh how that would make life easier, but alas, it
    really isn't possible. Why? You can't write async without threads, because the Unix
    API doesn't provide an asynchronous system call for every operation, meaning if you
    need to do something that might block, you must do it in a thread. And the list of
    stuff that might block is long and crippling. Want to read from a file without
    blocking out for SECONDS? Sorry! Not without a thread you don't. But once you've
    started a thread, all bets are off if you fork. POSIX actually doesn't even say
    anything about what happens to threads in a process that forks (besides saying they
    don't think its a good idea). In every sane OS, only the forking thread continues in
    the child, all the other threads are dead. OK, fine you say, let them die. But their
    mutexes, semaphores and condition variables are in whatever state they were in the
    moment you forked, that is to say, any state at all. Unfortunately this means that
    having created a thread that does anything meaningful (e.g. calls into libc), if you
    fork, all bets are off as to what happens in that child process. A dead thread may,
    for example, hold the lock around the C heap, which would mean that any call into libc
    would deadlock trying to allocate memory (oops), that'd ruin your day. Trust me, if
    parallel could work in some simpler way it would!

    Say I Want To Have a Giant Shared Matrix
    ----------------------------------------

    The parallelism model implemented is strictly message passing, shared memory isn't
    implemented, but there are various hacks you could use to make this work
    (e.g. implement it yourself). Bigarray already allows mmaping files, so in theory
    even a cluster of machines could all mmap a giant file and use/mutate it.

    Making Your Program Work on Multiple Machines
    ---------------------------------------------

    The library makes this pretty transparent, however there are a couple of things to
    watch out for if you want it to work seamlessly. First of all, your program should be
    able to run with no arguments. Ideally you'd call Parallel.init before parsing
    arguments, or at least you'd check to see if you're a worker machine before parsing
    arguments. The reason for this is that the library is going to copy your program to
    every machine in the cluster and start it up, it's going to set an environment
    variable, and Parallel.init is going to check that environment variable and do
    something completely different if it's set, in fact Parallel.init will never return in
    this scenario, but will instead become the master process for that machine. If you
    want to change your behavior based on whether you're running a worker machine or the
    master you can use Parallel.is_worker_machine. In general put Parallel.init as early
    in your program as possible.

    Try to avoid printing crazy things like sexps, or tons of data to stdout before
    calling Parallel.init. It uses stdout to communicate its address back to the master
    machine. The parser is pretty robust, and will toss out most things you print, but if
    you happen to print just the right sexp, it might think you're at the wrong
    address. This would just cause startup to hang, but would probably be hard to debug.

    The examples (in the examples directory) all work on multiple machines, if you're
    stumped for a template to follow.

    Why Can't I Use Async Before Parallel.init?
    -------------------------------------------

    By default Parallel.init does a check that you haven't created any threads, and that
    you haven't made any use of async. The threads check is mandatory, but the async check
    can be turned off by setting [fail_if_async_has_been_initialized] to false. Why is
    this check the default? Well in general you can't initialize async libraries before
    calling Parallel.init and expect them to work in the child process. The reason is that
    the async scheduler is thrown away in the child process before calling
    Scheduler.go. This is out of necessity, there is no way we can know what state the
    scheduler is in at the moment we fork, and it would be quite unfortunate if it were in
    a bad state, or worse, there are jobs on the queue that get run in all workers as soon
    as they call Scheduler.go. But as a result of this, any asyncy thing you create before
    Parallel.init won't work in worker processes. For example, say you initialize the log
    module before Parallel.init expecting to use it in the workers. It won't work, since
    all of its state (loops, writers, etc) is invalid in the worker processes. The check
    exists to make sure people are aware of this, and to make sure it only happens if they
    really know it's ok.

    What CWD Will Worker Machine Processes Have?
    --------------------------------------------

    If the CWD of the master exists on the worker machine, and you have permission to
    enter it, then parallel will switch to that directory before starting the master
    process, otherwise it will chdir to /.
*)

module Parallel = Intf
module Channel = Channel
module Hub = Hub
module Cluster = Import.Cluster

(* module Shm : sig
 *   type 'a descriptor
 *   val copy_to : 'a -> 'a descriptor Async.Deferred.t
 *   val reify : 'a descriptor -> 'a Async.Deferred.t
 * end = Shm *)

(* This toplevel module expression only runs in processes spawned by
   init_child_machines. *)
