/*
 * PolyHJ: Polymorphic Hash Join.
 * > Defines.
 *   (a) Timer Functions.
 *   (b) Barrier Functions.
 *   (c) Safe malloc() Family Wrapper Functions.
 *   (d) Misc. Math Functions.
 */


#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <pthread.h>
#include <assert.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

#include "common.h"

/*** Constants. ***/
#define MAXTIDS  2048
#define MAGICNUM 8


/*** Timer Functions. ***/

void timer_start(ttimer_t *T) {
  assert(clock_gettime(CLOCK_MONOTONIC_RAW, &( T->checkpoint )) == 0);
  T->elapsed = 0.0;
}

void timer_stop(ttimer_t *T) { timespec now;
  assert(clock_gettime(CLOCK_MONOTONIC_RAW, &now) == 0);
  T->elapsed = (double)(now.tv_sec  - T->checkpoint.tv_sec) * (1000.0*1000.0) +
               (double)(now.tv_nsec - T->checkpoint.tv_nsec) / (1000.0);
}

double timer_elapsed_sec(ttimer_t *T) {
  return T->elapsed / (1000.0 * 1000.0);
}

void timer_print(ttimer_t *T, char* msg) {
  printf("%s: %f sec.\n", msg, timer_elapsed_sec(T));
}


void global_timer_start(ttimer_t *t, uint32_t tid) {
  if(tid == 0) timer_start(t);
}

// global_timer_report() guarantees that it contains a synchronization barrier.
void global_timer_report(ttimer_t *t, uint32_t tid, char *msg) {
  barrier();
  if(tid == 0) { timer_stop(t); timer_print(t, msg); }
}


/*** Barrier Functions. ***/

static pthread_barrier_t TBarrier;
static uint8_t           Step[MAXTIDS];
static volatile uint16_t Barrier[MAGICNUM];

void barrier_init() {
  assert(Threads.N <= MAXTIDS);
  assert(MAGICNUM > 3);
  assert(MAGICNUM  < (1 << 8*sizeof(*Step)));
  assert(MAXTIDS   < (1 << 8*sizeof(*Barrier)));

  for(int t = 0; t < MAXTIDS;  t++) Step[t]    = 0;
  for(int k = 0; k < MAGICNUM; k++) Barrier[k] = 0;
  assert(pthread_barrier_init(&TBarrier, NULL, Threads.N) == 0);
}

void barrier() {
  int x = pthread_barrier_wait(&TBarrier);
  assert(x == 0 || x == PTHREAD_BARRIER_SERIAL_THREAD);
}

void sbarrier(short tid) {
  uint8_t  step = Step[tid];
  uint16_t w    = __sync_add_and_fetch(&( Barrier[step] ), 1); // Atomic.

  while(w != Threads.N) w = Barrier[step];

  __sync_synchronize();

  if(tid == 0) Barrier[ (step == 0) ? (MAGICNUM - 1) : (step - 1) ] = 0;
  Step[tid] = (Step[tid] + 1) % MAGICNUM;
}



/*** Safe malloc() Family Wrappers. ****/

void* SafeMalloc(size_t size) {
  void *p = malloc(size);
  assert( p != NULL );
  return p;
}

void* SafeCalloc(size_t nmemb, size_t size) {
  void *p = calloc(nmemb, size);
  assert( p != NULL );
  return p;
}

void* PageAlignedAlloc(size_t size) {
  void* p;
  assert(posix_memalign(&p, SysInfo.page_size, size) == 0);
  return p;
}

void* CacheLineAlignedAlloc(size_t size) {
  void* p;
  assert(posix_memalign(&p, SysInfo.line_size, size) == 0);
  return p;
}



/*** Misc. Math-related Functions. */

uint32_t lg_floor(uint32_t N) { assert(N >= 1);
  uint32_t result = 0;
  while(N >>= 1) result++;
  return result;
}

uint32_t lg_ceil(uint32_t N) { assert(N >= 1);
  uint32_t result = lg_floor(N);
  return result + ((1U << result) != N);
}

uint32_t div_ceil(uint32_t a, uint32_t b) {
  return (a / b) + ((a % b) > 0);
}
