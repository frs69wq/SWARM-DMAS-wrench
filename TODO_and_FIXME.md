## Remaining TODOs from Phase 1
- [ ] Step 3: Compute my own bid for the job
  - [ ] HeuristicBidding: Use a heuristic, based on JobDescription, HPCSystem Description, and current system state
    - [ ] @Prachi TODO: improve this heuristic (taking into account job and systems types for instance)
  - [x] PythonBidding: Call an external python script, e.g., calling an LLM
    - [ ] @Prachi IN PROGRESS: reuse some of the code from the demo
    - [ ] @Prachi indfo vs. debug in python scripts
- [ ] Step 5: Find a consensus on the winner of the competitive bidding for that job
  - [ ] Use a real distributed consensus algorithm
- [x] HPCSystem description
  - Remove MEMORY or GPU categories
    - [x] ~~Frontier has much more memory than other. Add a second category?~~
    - [x] GPU is redundant with `has_gpu`. Remove it?
- [x] Workload generation
  - [x] @Prachi TODO: check the consistency of the generated JSON files (246/1000 rejected jobs currently with PureLocal scheduling policy)
  - [x] @Prachi Jobs seems to be submitted in packets of more than 5. Feature or ~~bug~~? not touching at the moment.

## TODO -- Phase 2: Resilience!!!

## FIXME
