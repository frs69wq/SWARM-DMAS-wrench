#include "HeuristicBiddingSchedulingPolicy.h"
#include "PureLocalSchedulingPolicy.h"

#include <stdexcept>

std::shared_ptr<SchedulingPolicy> SchedulingPolicy::create_scheduling_policy(const std::string& policy_name)
{
  if (policy_name == "PureLocal")
    return std::make_shared<PureLocalSchedulingPolicy>();
  else if (policy_name == "HeuristicBidding")
    return std::make_shared<HeuristicBiddingSchedulingPolicy>();
  else
    throw std::invalid_argument("Unknown scheduling policy: " + policy_name);
}

void SchedulingPolicy::init_num_received_bids(int job_id)
{
  for (const auto& agent : job_scheduling_agent_network_)
    num_received_bids_[job_id].try_emplace(agent->getHostname(), 0);
}
