/*
 * PolyHJ: Polymorphic Hash Join.
 * In-place Cache-aware Partitioning (ICP).
 *
 * (a) Partitions a given sub-relation (assigned to a thread) of relation R or S
 * into 2^Radix.R or 2^Radix.S partitions, respectively, by re-ordering the
 * tuples within small equal-sized blocks into their partitions.
 *
 * (b) Estimates skew at the granularity of a partition in S, skipping its
 * partitioning if high skew is observed (determined by arbitrary thresholds)
 * and |S| is significantly larger than |R|.
 *     [See more detailed note about skew estimation within ICP().]
 */

#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include "common.h"

/* Function Declarations. */
bool ICP_estimate_skew(uint32_t, counter_t*, uint32_t);

/* Global Variables. */
uint32_t HighSkewObserved = 0;
bool     ChangedRadixS    = false;
uint8_t  ModelIII_shift;


void ICP(thread_t* Args, relation_t *Sub,
         uint32_t radix, block_meta_t *Blocks)
{
  if(radix == 0) return; /* Skip Partitioning. */

  /* Partitioning Parameters. */
  uint32_t shift  = 0;
  uint32_t fanout = 1 << radix;
  uint32_t mask   = fanout - 1;

  /* Under Model III, shift during hashing. */
  if(Sub->id == 'R' && Radix.S == 0) {
    shift = ModelIII_shift = lg_ceil(Threads.RelR->size) - Radix.R - 1;
  }

  /* Sub-Relation Info. */
  tuple_t *T          = Sub->tuples;
  uint32_t N          = Sub->size;

  /*
   * Blocks-related Info.
   * Each block has size avg_block_size, potentially plus one.
   */
  uint32_t num_blocks;
  Blocks->N = num_blocks    = div_ceil(N, ChunkSize);
  uint32_t avg_block_size   = N / num_blocks;
  uint32_t remainder        = N % num_blocks;
  uint32_t first_block_size = avg_block_size + (remainder > 0);

  /*
   * A block is divided into as many sub-blocks as there are LLC groups.
   * This way, x LLC groups can build x different hash tables in parallel,
   * each from a distinct set of partitions, belonging to a distinct sub-block
   * index in each block.
   * TODO: Implement support for cases where fanout(R/S) % num_sub_blocks != 0.
   *       In these cases, sub_block_partitions will have a remainder.
   *       I suppose this is only required for Model II, where f_R == f_S.
   *       Under Model II, we could save the "rest" of partitions in a
   *       separate sub-block (or, simpler but likely won't be enough,
   *       distribute remainder across sub-blocks).
   */
  uint32_t num_sub_blocks       = Threads.num_groups; // # of utilized LLC(s).
  uint32_t sub_block_partitions = fanout / num_sub_blocks;
  assert(fanout % num_sub_blocks == 0);

  /* Under Model IV, use one sub-block per block in partitioning relation S. */
  if(Sub->id == 'S' && Radix.R > Radix.S) num_sub_blocks = 1;

  /* Allocate and prepare block-position structures. */
  block_t **Pos;
  Blocks->Pos = Pos = SafeMalloc(num_blocks * sizeof(block_t*));
  block_t    *Array = SafeMalloc(num_sub_blocks * num_blocks * sizeof(block_t));

  for(uint32_t i = 0; i < num_blocks; i++) {
    Pos[i] = Array + (i * num_sub_blocks);
  }

  /* Allocate temporary ICP structures. */
  counter_t *Histo     = SafeMalloc(fanout * sizeof(counter_t));
  tuple_t   *TmpBlock = SafeMalloc(first_block_size * sizeof(tuple_t));

  /*
   * Directory to which current block's tuples are scattered.
   * Initially, this is set to a temporary buffer, TmpBlock.
   * Otherwise, it is set to the "previous" block.
   */
  tuple_t *Directory = TmpBlock;


  uint32_t block = 0;
  for(uint32_t i = 0; i < N;) {
    /* Current block's data. */
    uint32_t from   = i;
    uint32_t length = avg_block_size + (remainder > 0 && remainder--);
    uint32_t to     = from + length;

    assert(to <= N);
    assert( (block < num_blocks-1) || (to == N) );

    /* Fill the histogram with frequency of each partition in block. */
    for(uint32_t j = 0; j < fanout; j++) Histo[j] = 0;
    for(uint32_t j = from; j < to; j++) {
      ++Histo[ HASHx( T[j].key, mask, shift ) ];
    }

    /*
     * Skew Estimation.
     * When processing the first block in own sub-relation of S,
     * skew is estimated. If high skew is observed in S (and other criteria
     * are satisfied, e.g. |S| > a*|R|), the fanouts are changed and ICP
     * is restarted with the new fanout f_S (based on new Radix.S).
     * In terms of this code, Radix.S will be set to zero, thus stopping ICP.
     *
     * If the radix is user supplied, it will be left unchanged.
     */
    if(Sub->id == 'S' && block == 0 && !Radix.user_defined) {
      if(!ChangedRadixS &&
         ICP_estimate_skew(Args->tid, Histo, first_block_size))
      {
        // Cleanup.
        free(Pos);   free(Array);
        free(Histo); free(TmpBlock);

        // Restart ICP for relation S with new radix (if zero, ICP is stopped).
        ICP(Args, Sub, Radix.S, Blocks);
        return;
      }
    }

    /* Prepare prefix-sum array for partitions in block. */
    uint32_t accum = 0;
    for(uint32_t j = 0; j < fanout; j++) {
      uint32_t pre_accum = Histo[j];
      Histo[j] = accum;
      accum += pre_accum;
    }

    assert(Histo[0] == 0);

    /* Fill block's (and its sub-blocks') position information. */
    for(uint32_t m = 0; m < num_sub_blocks; m++) {
      uint32_t p = m * sub_block_partitions; // First partition within sub-block.
      uint32_t q = p + sub_block_partitions; // Last partition plus one.
      uint32_t r = (block == 0 ? N : from) - first_block_size; // Block offset.

      /* Set the start and end positions of the m'th sub-block of block. */
      Pos[block][m].start = r + Histo[p];
      Pos[block][m].end   = r + (q == fanout ? length : Histo[q]);
    }


    /* Scatter tuples to partitions, onto space in Directory. */
    for(; i < to; i++) {
      tuple_t  t = T[i];
      uint32_t h = HASHx(t.key, mask, shift);
      Directory[ Histo[h]++ ] = t;
    }

    assert(Histo[fanout-1] == (to - from));

    /* Next Block, next Directory. */
    if(Directory == TmpBlock) Directory = T;
    else                       Directory += Histo[fanout-1];
    block++;
  }

  /* Copy over the temporary buffer, TmpBlock, in place of last block. */
  assert(remainder == 0);
  assert(( T + N - Directory ) == first_block_size);
  memcpy(Directory, TmpBlock, first_block_size*sizeof(tuple_t));

  /* Cleanup. */
  free(Histo);
  free(TmpBlock);

  return;
}



/*
 * TODO: Describe Function.
 * Note arbitrary thresholds.
 * Note requires all threads to report high skew; somewhat conservative.
 */
bool ICP_estimate_skew(uint32_t tid, counter_t* Histo, uint32_t block_size) {
  uint32_t maxA = 0, maxB = 0;

  // Skip if |S| / |R| ratio is lower than threshold (3X).
  if(Threads.RelS->size / Threads.RelR->size < 3) return false;

  /* Find frequencies of the two most common partitions. */
  for(uint32_t j = 0; j < FanoutS; j++) {
    if(Histo[j] > maxA) { maxB = maxA; maxA = Histo[j]; }
    else if(Histo[j] > maxB) maxB = Histo[j];
  }

  uint32_t skew_threshold = block_size * 35 / 100; /* 35% in A,B of total. */

  /* If skew exceeds threshold locally, increment global counter. */
  if( (FanoutS >  4 && maxA+maxB > skew_threshold) ||
      (FanoutS <= 4 && maxA > (block_size / 2) + 10) )
  {
    __sync_fetch_and_add(&HighSkewObserved, 1);
  }

  // Wait for all threads to report skew.
  sbarrier(tid);

  /* Thread zero acts upon reported skew, if threads agreed upon high skew. */
  if(tid == 0 && HighSkewObserved == Threads.N) {
    ChangedRadixS = true;

    /* Print Message. */
    printf("#>> High skew observed. Switching to Model III with "
           "f_R = 2^(%d+1), f_S = 2^0.\n", Radix.R);

    /* Set to Model III, with double fanout for relation R. */
    Radix.S = 0;
    Radix.R = Radix.R + 1;
  }

  // Wait for new radix bits.
  sbarrier(tid);

  return (HighSkewObserved == Threads.N);
}



/*
 * Thread-local ICP cleanup.
 */
void ICP_cleanup(thread_t* Args) {
  if(Radix.R > 0) {
    free(*Args->BlocksR.Pos);
    free(Args->BlocksR.Pos);
  }

  if(Radix.S > 0) {
    free(*Args->BlocksS.Pos);
    free(Args->BlocksS.Pos);
  }
}
