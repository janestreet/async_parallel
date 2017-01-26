open Core

let tests =
  []
  @ Parallel_test.tests
;;

let () = Qtest_lib.Std.Runner.main tests
