#ifndef HEURISTIC_BIDDING_SCHEDULING_POLICY_H
#define HEURISTIC_BIDDING_SCHEDULING_POLICY_H

#include "ControlMessages.h"
#include "JobSchedulingAgent.h"
#include "SchedulingPolicy.h"

class HeuristicBiddingSchedulingPolicy : public SchedulingPolicy {

public:
  void broadcast_job_description(wrench::JobSchedulingAgent* self,
                                 const std::vector<std::shared_ptr<wrench::JobSchedulingAgent>>& peers,
                                 const std::shared_ptr<JobDescription> job_description) override
  {
    // The broadcast is only called upon initial submission, we thus init the number of received bids only once.
    init_num_received_bids(job_description->get_job_id());
    for (const auto& other_agent : peers)
      if (std::shared_ptr<wrench::JobSchedulingAgent>(self) != other_agent)
        other_agent->commport->dputMessage(new wrench::JobRequestMessage(job_description, false));
  }

  double
  compute_bid(const std::shared_ptr<JobDescription> job_description,
              const std::shared_ptr<HPCSystemDescription> hpc_system_description /*, hpc_system_status */) override
  {
    // TODO
    return 1.0;
  }

  void broadcast_bid_on_job_(wrench::JobSchedulingAgent* bidder,
                             const std::vector<std::shared_ptr<wrench::JobSchedulingAgent>>& peers,
                             const std::shared_ptr<JobDescription> job_description, double bid)
  {
    // Set the number of needed bids to the size of the peers vector
    set_num_needed_bids(peers.size());
    for (const auto& other_agent : peers)
      other_agent->commport->dputMessage(new wrench::BidOnJobMessage(bidder, job_description, bid));
  }

  bool did_win_bid(const std::vector<std::shared_ptr<wrench::JobSchedulingAgent>>& /* peers */,
                   double /*local_bid*/) const override
  {
    // TODO
    return true;
  }
};
#endif // HEURISTIC_BIDDING_SCHEDULING_POLICY_H
