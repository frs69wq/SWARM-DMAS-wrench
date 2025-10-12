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