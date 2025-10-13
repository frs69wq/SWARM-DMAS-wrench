#ifndef PURE_LOCAL_SCHEDULING_POLICY_H
#define PURE_LOCAL_SCHEDULING_POLICY_H

#include "SchedulingPolicy.h"

class PureLocalSchedulingPolicy : public SchedulingPolicy {

public:
  void broadcast_job_description(const std::string& /* agent_name */,
                                 const std::shared_ptr<JobDescription>& job_description) override
  {
    // Function is only called upon initial submission, hence init the number of needed and received bids only once
    // Set the number of needed bids to 1
    set_num_needed_bids(1);
    init_num_received_bids(job_description->get_job_id());
  }

  double
  compute_bid(const std::shared_ptr<JobDescription>& /* job_description */,
              const std::shared_ptr<HPCSystemDescription>& /*hpc_system_description*/ /*, hpc_system_status */) override
  {
    return 1.0;
  }

  void broadcast_bid_on_job(const std::shared_ptr<wrench::S4U_Daemon>& bidder,
                            const std::shared_ptr<JobDescription>& job_description, double bid)
  {
    // Just sends a BidOnJobMessage to itself
    bidder->commport->dputMessage(new wrench::BidOnJobMessage(bidder, job_description, bid));
  }

  std::shared_ptr<wrench::JobSchedulingAgent>
  determine_bid_winner(const std::map<std::shared_ptr<wrench::JobSchedulingAgent>, double>& all_bids) const override
  {
    return all_bids.begin()->first;
  }
};
#endif // PURE_LOCAL_SCHEDULING_POLICY_H
