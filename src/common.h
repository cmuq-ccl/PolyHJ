/*
 * PolyHJ: Polymorphic Hash Join.
 * > Includes:
 *   > SysInfo, Threads, Radix [extern global vars]
 *   > Type Definitions [types.h]
 *   > Common functions declarations.
 * > Defines:
 *   > FanoutR/S, MaskR/S
 *   > LINEMAX
 *   > MIN/MAX(x, y)
 *   > HASH/HASHx()
 *   > randgen(max, G)
 */

#ifndef __COMMON_H__
  #define __COMMON_H__

  #include <stdint.h>
  #include <stdbool.h>
  #include <assert.h>
  #include <limits.h>

  #include "util/sys_info.h"
  #include "types.h"

  /* Fanout-related MACROs. */
  #define FanoutR (1 << Radix.R)
  #define FanoutS (1 << Radix.S)
  #define MaskR   (FanoutR - 1)
  #define MaskS   (FanoutR - 1)

  /* Constants. */
  #define LINEMAX   4096
  #define TEST_KEY_INPLACEOF_PAYLOAD false
  #define ChunkSize ((1 << 15) - 10)

  #if ChunkSize < (1 << 16)
    typedef uint16_t counter_t;
  #else
    typedef uint32_t counter_t;
  #endif

  /* Helper MACROs. */
  #define MIN(X, Y) (((X) < (Y)) ? (X) : (Y)) /* Beware double-evaluation! */
  #define MAX(X, Y) (((X) > (Y)) ? (X) : (Y))
  #define HASH(K, MASK)  (K & MASK)
  #define HASHx(K, MASK, SHIFT) ((K >> SHIFT) & MASK)

  /* External Global Variables. */
  extern sys_info_t   SysInfo;   // Hardware Stats, etc.
  extern params_t     Threads;
  extern radix_info_t Radix;

  /*** Function Declarations. ***/
  // Running Threads.N threads, each passed its own Threads.Args.
  void run_threads(void* (*f) (void*));

  // Timers.
  void   timer_start(ttimer_t*);
  void   timer_stop(ttimer_t*);
  double timer_elapsed_sec(ttimer_t*);
  void   timer_print(ttimer_t*, char*);
  void   global_timer_start(ttimer_t*,  uint32_t);
  void   global_timer_report(ttimer_t*, uint32_t, char*);

  // Barriers.
  void barrier_init();
  void barrier();
  void sbarrier(short tid);

  // Allocation.
  void* SafeMalloc(size_t);
  void* SafeCalloc(size_t, size_t);
  void* PageAlignedAlloc(size_t);
  void* CacheLineAlignedAlloc(size_t);

  // Misc Math-related Functions.
  uint32_t lg_floor(uint32_t);
  uint32_t lg_ceil(uint32_t);
  uint32_t div_ceil(uint32_t, uint32_t);


  /*** Inline Function Definitions: Random Number Generator. ***/
  /* Link: https://en.wikipedia.org/wiki/Xorshift */
  static inline uint32_t xorshift128(randgen_t *G) {
      uint32_t t = G->x;
      t ^= t << 11;
      t ^= t >> 8;
      G->x = G->y; G->y = G->z; G->z = G->w;
      G->w ^= G->w >> 19;
      G->w ^= t;
      return G->w;
  }

  /* Link: http://funloop.org/post/2015-02-27-removing-modulo-bias-redux.html */
  static inline uint32_t randgen(uint32_t max, randgen_t *G) {
    uint32_t r;
    uint32_t threshold = -max % max;
    while ((r = xorshift128(G)) < threshold);
    return r % max;
  }


#endif
