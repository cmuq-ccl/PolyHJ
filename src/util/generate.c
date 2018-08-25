/*
 * PolyHJ: Polymorphic Hash Join.
 *
 * Input Relations Generation:
 *
 * (a) Thread Functions that allocate, generate and NUMA-distribute relations.
 *     These can be used as input to run_threads().
 *
 * (b) Generation Functions that produce uniform R, uniform S and skewed S.
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>

#include "common.h"

/* Global Variables. */
static randgen_t G;
static bool      create_R_first = true;

/* Generation Functions Declarations. */
void fill_primary_keys(relation_t*);
void fill_skewed_keys (relation_t*, relation_t*);
void fill_foreign_keys(relation_t*, relation_t*);


/*
 * Allocates, generates and NUMA-distributes a given relation.
 * NOTE: Frees Rel->tuples.
 */
void create_rel(uint32_t tid, relation_t *Rel, relation_t *Sub) {
  char id = Rel->id; // Relation 'R' or 'S'?

  /* Allocate relation. */
  if(tid == 0) Rel->tuples = PageAlignedAlloc(Rel->size * sizeof(tuple_t));

  /* NUMA Distribute [important, even as we localize below]. */
  barrier();
  memset(Rel->tuples + Sub->offset, 0, sizeof(tuple_t) * Sub->size);
  barrier();

  /* Fill in a shuffled array of tuples. */
  if(tid == 0 && id == 'R') fill_primary_keys(Rel);
  if(tid == 0 && id == 'S') {
    if(Rel->skew > 0.0) fill_skewed_keys (Threads.RelR, Threads.RelS);
    else                fill_foreign_keys(Threads.RelR, Threads.RelS);
  }

  // All threads wait for thread zero's generation of relation.
  barrier();

  /* NUMA Localize. */
  for(int t = Threads.N - 1; t >= 0; t--) {
    if(t == tid) {
      Sub->tuples = SafeMalloc(sizeof(tuple_t) * Sub->size);
      memcpy(Sub->tuples, Rel->tuples + Sub->offset, sizeof(tuple_t)*Sub->size);
      Rel->tuples = realloc(Rel->tuples, Sub->offset * sizeof(tuple_t));
    }

    barrier();
  }


  return;
}


/*
 * Frees the allocations by create_rel().
 * Note that Threads.RelR->tuples and Threads.RelS->tuples are freed by
 * realloc().
 */
void create_rel_cleanup() {
  for(uint32_t t = 0; t < Threads.N; t++) {
    free(Threads.Args[t].SubR->tuples);
    free(Threads.Args[t].SubS->tuples);
  }
}



/*
 * Thread function to create relation R.
 */
void *create_R(void* params) {
  thread_t   *T = (thread_t*)params;

  /*
   * If |S| is very large and skewed, create it first so that R is not deemed
   * by the kernel as unused (and hence potentially swapped out).
   * This ocassionally occurs (esp. on limited DRAM) since creating a large
   * skewed relation takes much time.
   */
  if(Threads.RelS->size > 16U*128*1000*1000 && Threads.RelS->skew > 0.0) {
    if(T->tid == 0) create_R_first = false; // Synchronized by pthread_join()
    return NULL;
  }

  /* Allocate, generate and NUMA-distribute relation R. */
  create_rel(T->tid, Threads.RelR, T->SubR);

  return NULL;
}


/*
 * Thread function to create relation S.
 */
void *create_S(void* params) {
  thread_t   *T = (thread_t*)params;

  /* Allocate, generate and NUMA-distribute relation R. */
  create_rel(T->tid, Threads.RelS, T->SubS);

  /* See create_R for more information. */
  if(!create_R_first) create_rel(T->tid, Threads.RelR, T->SubR);

  return NULL;
}


/*
 * Seeds the global G structure.
 */
static void seed(uint32_t seed) {
  G.w = 67819 + seed;  G.x = 2     + seed;
  G.y = 138   + seed;  G.z = 9127  + seed;
}


/*
 * Fills T with a random permutation of the numbers [1, N] inclusive.
 */
void permutation(tuple_t *T, uint32_t N) { if(N == 0) return;
  for(uint32_t i = 0; i < N; i++) T[i].key = i+1;

  for(uint32_t i = N-1; i > 0; i--) {
    uint32_t j = randgen(i, &G);
    tkey_t tmp = T[i].key;
    T[i].key   = T[j].key;
    T[j].key   = tmp;
  }

  return;
}


/*
 * Fills the tuples in RelR with shuffled primary keys.
 */
void fill_primary_keys(relation_t* RelR) {
  seed(RelR->seed);
  permutation(RelR->tuples, RelR->size);
  return;
}


/*
 * Fills the tuples in RelS with shuffled uniform foreign keys.
 */
void fill_foreign_keys(relation_t* RelR, relation_t* RelS) {
  seed(RelS->seed);

  uint32_t ratio     = RelS->size / RelR->size;
  for(uint32_t i = 0; i < ratio; i++) {
    permutation(RelS->tuples + i*RelR->size, RelR->size);
  }

  uint32_t remainder = RelS->size % RelR->size;
  permutation(RelS->tuples + ratio*RelR->size, remainder);

  return;
}


/*
 * Fills the tuples in RelS with random foreign keys following a skewed
 * Zipfian distribution, with z = RelS->skew.
 * Based off algorithm used by Balkesen et al.
 * (http://www.systems.ethz.ch/projects/paralleljoins)
 * as implemented by Jens Teubner
 * (itself derived from code originally written by Rene Mueller).
 */
void fill_skewed_keys(relation_t* RelR, relation_t* RelS) {
  seed(RelS->seed);
  srand(RelS->seed);

  double d = 0, s = 0;
  double z = RelS->skew;

  /* Produce a random permutation of all keys. */
  uint32_t *Keys = SafeMalloc(RelR->size * sizeof(uint32_t));
  for(uint32_t i = 0; i < RelR->size; i++) Keys[i] = i+1;
  for(uint32_t i = RelR->size-1; i > 0; i--) {
    uint32_t j = randgen(i, &G);
    /* uint32_t j = (uint64_t) i * rand() / RAND_MAX; */

    tkey_t tmp = Keys[i];
    Keys[i]    = Keys[j];
    Keys[j]    = tmp;
  }

  /* Produce a lookup table. */
  double *Table = SafeMalloc(RelR->size * sizeof(double));
  for(uint32_t i = 0; i < RelR->size; i++) d += 1.0 / pow(i+1, z);
  for(uint32_t i = 0; i < RelR->size; i++) {
    s += 1.0 / pow(i+1, z);
    Table[i] = s / d;
  }

  /* Fill Relation S. */
  for(uint32_t i = 0; i < RelS->size; i++) {
    uint32_t l = 0, r = RelR->size - 1;

    double x = ((double) rand()) / RAND_MAX;
    /* double x = ((double) randgen(INT_MAX, &G)) / UINT_MAX; */

    if(Table[0] >= x) r = 0;

    while(r - l > 1) { uint32_t m = l + (r - l) / 2;
      if (Table[m] < x) l = m; else r = m;
    }

    RelS->tuples[i].key = Keys[r];
  }


  /* Cleanup. */
  free(Keys);
  free(Table);

  return;
}
