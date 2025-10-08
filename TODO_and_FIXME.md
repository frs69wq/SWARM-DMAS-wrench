TODO
 - [-] Decide how to enforce local execution when we don't want to use the network of JSAs
       - [ ] Option 1: use the 'can_forward' member of a JobRequestMessage
       - [X] Option 2: implement a default behavior for a JSA that consists in directly submitting the job to the local
                       HPC system. 
 - [ ] Implement a flexible workflow in JobSchedulingAgent::processEventCustom
       - [ ] Have steps 1 to 3 below as separate functions
            - [ ] Idea: Define a SchedulingPolicy abstract class that requires the following 3 steps. A specific policy
                  would then instantiate a child class with specific functions. The scheduling policy is assigned to 
                  JSAs at the beginning of the simulation.
                  - Example: A default policy that does not rely on the agent network would have no-op functions for
                             steps 1 to 3 and just perform step 4 directly
                  - Example: Step 2 can use either heuristic- or LLM-based bidding leading to different policies   
       - [ ] Step 1: Broadcast the jobDescription to the network of JSAs
            - [ ] test the value of can_forward_ to determine if the job is an original submission from the WSA or a 
                  forward from a JSA
            - [ ] if true then broadcast the JobDescription to the other agents (with can_forward set to false)
            - [ ] if false
                - [ ] Option 1: Move to step 2
                - [ ] Option 2 (resilient): Send ack to sender (has code modification implications)
                    - [ ] this includes adding a step to wait for the acks and handle agents not responding (timeout)
       - [ ] Step 2: Compute my own bid for the job
            - [ ] Option 1: Do nothing
            - [ ] Option 2: Use a heuristic, based on JobDescription, HPCSystem Description, and current system state
            - [ ] Option 3: Call a LLM
       - [ ] Step 3: Find a consensus on the winner of the competitive bidding for that job
            - [ ] Option 1: Do nothing
            - [ ] Option 2: Send bid to all other JSAs, then decide of winner locally based on all values
                - [ ] Manage tie breaking
            - [ ] Option 3:
       - [ ] Step 4: Upon winning the competitive bidding, schedule the job on my local HPC system
 - [ ] Create an HPCSystem Class that contains a static high level description of the system
       - [ ] Decide of the information to have
       - [ ] Add them to the platform description 
       - [ ] Create the .h file
       - [ ] Instantiate at parsing time
       - [ ] Pass it to the JSA at creation time
FIXME
 - [ ] Decide whether job completion notifications are sent to the WSA (current) or handled locally by the JSAs.
       This impacts most of the code with the "originator" thing
 - [ ] Directly pass the JobDescription in a JobRequestMessage and only parse/transform when needed to submit job
       locally