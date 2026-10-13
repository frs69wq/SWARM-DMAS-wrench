# Q1

```text
Q1 wait-time reduction vs PureLocal
Reductions are computed per matched scenario, then summarized. Median and IQR show the typical reduction and spread; mean +/- std is included for comparison.
                            mean_seconds_reduced  std_seconds_reduced  mean_pct_reduced  std_pct_reduced
strategy                                                                                                
HeuristicBidding                        27571.10             14267.07             72.94            14.28
EmbeddingBidding_static0.2              25855.78             13385.63             71.02            18.82
EmbeddingBidding_static0.0              25523.08             13217.89             70.11            18.65
EmbeddingBidding_static0.5              25122.77             15427.71             67.25            20.30

Q1 matched scenarios used: 24/24 scenarios where every strategy has results.
```

# Q2/Q3

```text
Q2/Q3 HeuristicBidding wait-time heatmap values
Values are mean reductions vs PureLocal across balanced workload types. These are the HeuristicBidding cells from the Waiting heatmap, shown with the raw PureLocal wait time for context.
   arrival_pattern  rho  purelocal_wait_seconds  seconds_reduced  pct_reduced
bursty_high_stress 0.90                31343.18         21283.14        70.83
bursty_high_stress 1.50                54718.27         32908.97        58.47
 bursty_low_stress 0.90                27620.80         24070.67        88.71
 bursty_low_stress 1.50                48229.95         32802.36        68.17
          business 0.90                27142.73         24293.45        88.65
          business 1.50                48352.17         30067.99        62.81

Largest absolute wait reduction: bursty_high_stress, rho=1.5: 32908.97s (58.47%).
Smallest absolute wait reduction: bursty_high_stress, rho=0.9: 21283.14s (70.83%).
```

# Q4

```text
Q4 decision-time overhead vs PureLocal
Values are mean extra decision seconds relative to PureLocal across the matched Q4 scenarios.
                  strategy  mean_seconds_added
          HeuristicBidding            0.000233
EmbeddingBidding_static0.2            0.001285
EmbeddingBidding_static0.0            0.001357
EmbeddingBidding_static0.5            0.001415
```

# Q6

```text
Q6 fast-machine work share
Fast-machine work share is occupied node-seconds on the fastest systems divided by total occupied node-seconds.
Fastest systems: Frontier, Aurora.

               method  all_scenarios_share_pct  timeline_final_share_pct  max_over_capacity_pct
Predefined Allocation                    52.72                     48.88                   0.00
    Heuristic Bidding                    86.88                     88.44                   0.00
```

# Q7

```text
Q7 exact job-system type matches
Counts show how many scheduled jobs each method placed on a system with the same type as the job.
               method  scheduled_jobs  exact_job_matches  exact_match_pct
Predefined Allocation           26010               8040            30.91
    Heuristic Bidding           26010               8727            33.55

Most exact matches: Heuristic Bidding with 8727 exact matches (33.55%).
```
