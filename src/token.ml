open Core

(** Token is a mechanism to detect when an object is not in the process where it was
    created. Every process has a token, which occupies some area of physical
    memory. Every object we wish to track has a pointer to it's processes token. Checking
    whether the object is being used in another process than the one that created it is
    then a simple matter of checking physical equality of the executing processes' token
    against the token stored in the object. If they don't match, then the object has been
    moved to another process. This method avoids the pitfalls of using pids, which are
    reused on a short time scale by the OS. *)

type t = {v: unit}

(** It's true that because we allocate this block at module init time every worker process
    will have [mine] at the same address. However this is ok, because when we marshal
    something it will make a deep copy of mine, and so we can still detect that we're not
    in the same process as where we were created. *)
let mine = {v = ()}
let valid tok = phys_equal tok mine

