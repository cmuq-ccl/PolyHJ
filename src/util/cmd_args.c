/*
 * PolyHJ: Polymorphic Hash Join.
 * > extract_cmd_args(int, char**) parses arguments:
 *   (a) --threads: Number of threads to run the join
 *   (b) --r, --s:  Number of tuples in relations R and S
 *   (c) --skew:    Zipfian skew factor for relation S
 *   (d) --radix, --radixR, --radixS: Set both/one fanout(s) to 2^r for given r
 *   (e) --favor_hyperthreading (flag)
 *   (f) --sched:   TODO.
 *   (g) --help:    TODO.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "common.h"

void extract_cmd_args(int argc, char **argv) {
  char buffer[LINEMAX];
  uint32_t ival;
  double   dval;
  char c;

  // False indicates user has not supplied --radix, --radixR or --radixS (yet).
  Radix.user_defined = false;

  /* For each user-supplied argument. */
  for(uint32_t  i = 1; i < argc; i++) {
    uint32_t pos = 0;
    memset(buffer, 0, LINEMAX); // Now, all char`s are '\0'.

    while((c = *(argv[i]))) { argv[i]++;
      if(c == '-') continue; /* Skip dashes. */
      if(c == '=') break;    /* Stop on ``=``. */

      assert(pos < LINEMAX-1); // Allowing one byte for the NULL terminator.
      buffer[pos++] = c;
    }

    /* Compare against possible options. */
    if(!strcmp(buffer, "threads") && sscanf(argv[i], "%u", &ival)) {
        Threads.N = ival;
      }

      else if(!strcmp(buffer, "r") && sscanf(argv[i], "%u", &ival)) {
        Threads.RelR->size = ival;
      }

      else if(!strcmp(buffer, "s") && sscanf(argv[i], "%u", &ival)) {
        Threads.RelS->size = ival;
      }

      else if(!strcmp(buffer, "skew") && sscanf(argv[i], "%lf", &dval)) {
        Threads.RelS->skew = dval;
      }

      else if(!strcmp(buffer, "radix") && sscanf(argv[i], "%u", &ival)) {
        Radix.user_defined = true;
        Radix.R = Radix.S = ival;
      }

      else if(!strcmp(buffer, "radixR") && sscanf(argv[i], "%u", &ival)) {
        Radix.user_defined = true;
        Radix.R = ival;
      }

      else if(!strcmp(buffer, "radixS") && sscanf(argv[i], "%u", &ival)) {
        Radix.user_defined = true;
        Radix.S = ival;
      }

      else if(!strcmp(buffer, "sched")) {
        // TODO:
        // > Read one character off supplied value.
        // > If h (i.e., hypertight), set Threads.favor_physical_cores to false
        // > If l (i.e., loose), set Threads.use_all_llcs to true [create it.]
        // > If t (i.e., tight), do nothing -- that's the default.
        // > Otherwise, report error.
        // > Remove option --favor_hyperthreading. Becomes redundant.
        printf("TODO.\n");
        exit(0);
      }

      else if(!strcmp(buffer, "favor_hyperthreading")) {
        Threads.favor_physical_cores = false;
        // This option means the following:
        // When assigning threads to CPUs, favor using hyperthreads on few
        // LLC(s) over assigning threads to physical cores across more LLC(s).
      }

      else if(!strcmp(buffer, "h") || !strcmp(buffer, "help")) {
        printf("TODO. Refer to src/util/cmd_args.c for arguments.\n");
        exit(0);
      }

      else if(buffer[0] != '\0') {
        printf(">> Unrecognized option/value for option ``%s``.\n", buffer);
      }
  }

  return;
}
