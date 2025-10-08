TODO
 - [ ] Decide how to enforce local execution when we don't want to use the network of JSAs
       - [ ] Option 1: use the 'can_forward' member of a JobRequestMessage
       - [ ] Option 2: implement a default behavior for a JSA that consists in directly submitting the job to the
             local HPC system. 

FIXME
 - [ ] Decide whether job completion notifications are sent to the WSA (current) or handled locally by the JSAs.
       This impacts most of the code with the "originator" thing