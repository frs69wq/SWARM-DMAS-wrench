#ifndef HEURISTIC_BIDDING_SCHEDULING_POLICY_H
#define HEURISTIC_BIDDING_SCHEDULING_POLICY_H

#include "SchedulingPolicy.h"

class HeuristicBiddingSchedulingPolicy : public SchedulingPolicy {

public:
  void broadcast_job_description(const std::vector<std::shared_ptr<wrench::JobSchedulingAgent>>& peers,
                                 const std::shared_ptr<JobDescription> job_description) override
  {
    // TODO
  }
  double
  compute_bid(const std::shared_ptr<JobDescription> job_description,
              const std::shared_ptr<HPCSystemDescription> hpc_system_description /*, hpc_system_status */) override
  {
    // TODO
    return 1.0;
  }
  bool did_win_bid(const std::vector<std::shared_ptr<wrench::JobSchedulingAgent>>& /* peers */,
                   double /*local_bid*/) const override
  {
    // TODO
    return true;
  }
};
#endif // HEURISTIC_BIDDING_SCHEDULING_POLICY_H
