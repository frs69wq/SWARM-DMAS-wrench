## Remaining TODOs from Phase 1
- [ ] Bidding
  - [ ] HeuristicBidding: Use a heuristic, based on JobDescription, HPCSystem Description, and current system state
    - [ ] @Prachi TODO: improve this heuristic (taking into account job and systems types, the estimated start time of the job, can we use user and group id, ...)
  - [x] PythonBidding: Call an external python script, e.g., calling an LLM
    - [ ] @Prachi IN PROGRESS: reuse some of the code from the demo
    - [ ] @Prachi info vs. debug in python scripts
    - [ ] @Prachi fix Claude bidder (chat -> messages, sensitive info as environment variables, ...)
    - [ ] @Prachi have the same code as in HeuristicBidding in a python script
    - [ ] @Prachi for all LLM-bidding, call this heuristic python script as a fallback

- [ ] Consensus on the winner of the competitive bidding
  - [x] Implement non-deterministic tie breaking (keep the one based on pointer address a second safety tie breaker)
  - [ ] Use a real distributed consensus algorithm
  - [ ] @Prachi: extract the consensus protocol from Komal's code to see if we can integrate

- [x] HPCSystem description
  - Remove MEMORY or GPU categories
    - [x] ~~Frontier has much more memory than other. Add a second category?~~
    - [x] GPU is redundant with `has_gpu`. Remove it?

- [x] Workload generation
  - [x] @Prachi TODO: check the consistency of the generated JSON files (246/1000 rejected jobs currently with PureLocal scheduling policy)
  - [x] @Prachi Jobs seems to be submitted in packets of more than 5. Feature or ~~bug~~? not touching at the moment.
  - [ ] @Prachi modify generation of submission times to cover a 27 hours period and have 6 different gaussian distributions (1 per system) with time zone differences. The maximum number of jobs concurrently submitted at the peak of the gaussian should be related to the total number of nodes of each system

## TODO -- Phase 2: Resilience!!!
- [x] Add a heartbeat monitor to control the state of the scheduling agents (one per HPC system)
  - [x] On each system, attach the heartbeat monitor to the job scheduling agent
  - [x] If the job scheduling agent is killed (simulating a software failure), its heartbeat monitor is killed too ... and can thus not send heartbeats anymore
    - [ ] See how to restart both
  - [x] `period` and `expiration` are harcoded to 5 and 15 seconds respectively (missing 2 heartbeats and then warn the scheduling agent that an agent is not responding). 
    - [x] Could be command line arguments instead
  - [X] Warn the scheduling agent when another system doesn't send its hearbeat
    - [x] have the scheduling agent listen for the messages from the heartbeat monitor
    - [ ] Do something with it! What?
      - [x] Have the scheduling policy object (now one per agent) maintain two set of agents, healthy and failed ones. 
      - [x] When a job scheduling agent is notified of missing heartbearts from another agent, it moves it from the healthy set to the failed set. 
    - [ ] Shall we warn the workload submission agent too?
- [ ] Add a resource switching agent that can turn hardware resources on and off
  - [ ] 

## FIXME
- [x] gather all the command line arguments of a simulation run into a json file.
- [x] Add R stuff to the Dockerfile so that the plots can be generated in the container.
- [ ] Split ControlMessages.h into multiple files. 