# BufferQueue: Architecture & Concurrency Mechanics

### Core Terminology
-- Design Pattern -- Lock-Free Bounded MPMC/SPSC Circular Queue
-- Core Strategy -- Atomic Reservation (Ticket-based allocation) with a Ready-Bit Signaling Mask
-- Memory Footprint -- Hyper-lean, zero-allocation, const-generic backed array

### The Synchronization Problem (The De-sync Gap)
-- Problem -- Modifying head/tail pointers is instantaneous via atomic CPU instructions
-- Problem -- Copying data into a memory slot takes time (non-instantaneous)
-- De-sync Gap -- Tail increments to the next ticket before the actual data is written to the slot
-- Risk -- A consumer thread could see `tail > head`, attempt to read the slot, and ingest uninitialized garbage data

### The Solution: Multi-Phase State Machine
-- Phase 1 (Reservation) -- Producers race to claim a slot by atomically incrementing `tail` (via CAS loop or fetch_add)
-- Phase 2 (Write/Isolation) -- Producer owns that exclusive index slot and writes the data in isolation while other threads continue racing
-- Phase 3 (Commit/Signaling) -- Producer flips a dedicated bit in the `ready` mask corresponding to that slot index
-- Phase 4 (Validation) -- Consumer reads `head`, matches it against the `ready` mask bit, and spins/backs off if the bit is 0 (even if `tail` is far ahead)



### Why the Bit Mask (Capacity N) is Mandatory
-- Wrap-around Indexing -- Maps infinite monotonic counters (`head`/`tail`) down to physical array bounds (`0` to `N-1`)
-- Performance -- Replaces slow CPU division/modulo instructions (`tail % N`) with blazing fast bitwise AND (`tail & (N - 1)`)
-- Concurrency Buffer -- Allows `tail` and `head` to grow past `N` to accurately compute queue full/empty status (`tail.wrapping_sub(head) >= N`) without breaking continuity

### Memory Ordering Guardrails
-- Push Side (Release) -- `ready.fetch_or(..., Ordering::Release)` acts as a memory barrier -- guarantees data write is visible to other cores BEFORE the ready bit flips
-- Pop Side (Acquire) -- `ready.load(Ordering::Acquire)` acts as a memory barrier -- guarantees no speculative reads of the data happen until the ready bit reads as 1
-- Invalidate/Drop Safety -- `head.swap(INVALID)` instantly halts all threads -- scans the `ready` mask to safely drop only fully-initialized slots, avoiding dropping uninitialized garbage memory

