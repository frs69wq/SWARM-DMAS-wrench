#ifndef PURE_LOCAL_SCHEDULING_POLICY_H
#define PURE_LOCAL_SCHEDULING_POLICY_H

#include "SchedulingPolicy.h"

class PureLocalSchedulingPolicy : public SchedulingPolicy {

public:
  void broadcast_job_description(wrench::JobSchedulingAgent* /*self*/,
                                 const std::vector<std::shared_ptr<wrench::JobSchedulingAgent>>& /* peers */,
                                 const std::shared_ptr<JobDescription> job_description) override
  {
    // Function is only called upon initial submission, hence init the number of needed and received bids only once
    // Set the number of needed bids to 1
    set_num_needed_bids(1);
    init_num_received_bids(job_description->get_job_id());
  }

  double
  compute_bid(const std::shared_ptr<JobDescription> job_description,
              const std::shared_ptr<HPCSystemDescription> hpc_system_description /*, hpc_system_status */) override
  {
    return 1.0;
  }

  void broadcast_bid_on_job_(wrench::JobSchedulingAgent* bidder,
                             const std::vector<std::shared_ptr<wrench::JobSchedulingAgent>>& peers,
                             const std::shared_ptr<JobDescription> job_description, double bid)
  {
    // Just sends a BidOnJobMessage to itself
    bidder->commport->dputMessage(new wrench::BidOnJobMessage(bidder, job_description, bid));
  }

  bool did_win_bid(const std::vector<std::shared_ptr<wrench::JobSchedulingAgent>>& /* peers */,
                   double /*local_bid*/) const override
  {
    return true;
  }
};
#endif // PURE_LOCAL_SCHEDULING_POLICY_H
