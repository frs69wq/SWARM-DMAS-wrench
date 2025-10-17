## Remaining TODOs from Phase 1
- [ ] Step 3: Compute my own bid for the job
  - [ ] HeuristicBidding: Use a heuristic, based on JobDescription, HPCSystem Description, and current system state
    - [ ] @Prachi: improve this heuristic (taking into account job and systems types for instance)
  - [ ] PythonBidding: Call an external python script, e.g., calling an LLM
    - [ ] @Prachi: reuse some of the code from the demo
- [ ] Step 5: Find a consensus on the winner of the competitive bidding for that job
  - [ ] Use a real distributed consensus algorithm
- [ ] HPCSystem description
  - We have no MEMORY or GPU systems
    - [ ] Frontier has much more memory than other. Add a second category?
    - [ ] GPU is redundant with `has_gpu`. Remove it?
- [ ] Workload generation
  - [ ] @Prachi check the consistency of the generated JSON files (246/1000 rejected jobs currently with PureLocal scheduling policy)
  - [ ] @Prachi Jobs seems to be submitted by packets of more than 5. Feature or bug? 

## TODO -- Phase 2: Resilience!!!

## FIXME