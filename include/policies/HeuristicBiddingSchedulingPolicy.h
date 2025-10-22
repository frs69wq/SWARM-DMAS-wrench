#ifndef HEURISTIC_BIDDING_SCHEDULING_POLICY_H
#define HEURISTIC_BIDDING_SCHEDULING_POLICY_H

#include <algorithm>

#include "agents/JobSchedulingAgent.h"
#include "messages/ControlMessages.h"
#include "policies/SchedulingPolicy.h"
#include "utils/utils.h"

class HeuristicBiddingSchedulingPolicy : public SchedulingPolicy {

public:
  void broadcast_job_description(const std::string& agent_name,
                                 const std::shared_ptr<JobDescription>& job_description) override
  {
    // The broadcast is only called upon initial submission, we thus init the number of received bids only once.
    init_num_received_bids(job_description->get_job_id());
    for (const auto& other_agent : get_job_scheduling_agent_network())
      if (agent_name != other_agent->getName())
        other_agent->commport->dputMessage(new wrench::JobRequestMessage(job_description, false));
  }

  double compute_bid(const std::shared_ptr<JobDescription>& job_description,
                     const std::shared_ptr<HPCSystemDescription>& hpc_system_description,
                     const std::shared_ptr<HPCSystemStatus>& hpc_system_status) override
  {
    // TODO replace by a heuristic based on job description, system description, and system status
    // 1. Feasibility check: if the job does not pass the acceptance tests, bid is 0.0
    // TODO test storage in acceptance tests
    if (do_not_pass_acceptance_tests(job_description, hpc_system_description))
      return 0.0;

    // 2. Utilization-based scores
    auto node_score =
        1 - (hpc_system_description->get_num_nodes() - hpc_system_status->get_current_num_avaibable_nodes()) /
                (1.0 * hpc_system_description->get_num_nodes());
    // FIXME: this is equivalent to the previous score
    // mem_score  = 1 - (machine.used_memory / machine.total_memory)
    // TODO see if we can add storage (not a the moment)
    // stor_score = 1- (machine.used_storage / machine.total_storage)

    // 3. Compatibility
    auto node_compat =
        std::min(1.0, hpc_system_status->get_current_num_avaibable_nodes() / (1.0 * job_description->get_num_nodes()));
    // FIXME: again this is equivalent to the previous score
    // mem_compat  = min(1.0, machine.available_memory / job.memory)
    // TODO see if we can add storage (not a the moment)
    // storage_compat = min(1.0, machine.available_storage / job.storage)

    // 4. Machine type compatibility
    // FIXME this is part of the acceptance tests and leads to 0.0 (job cannot execute)
    // if job.requires_gpu and not machine.has_gpu:
    //     type_score = 0.2
    // else:
    //     type_score = 1.0

    // 5. Queue length factor
    auto queue_factor = std::max(0.1, 1 - 0.1 * hpc_system_status->get_queue_length());

    // 6. Combine (basic multiplicative form)
    auto base_score = node_score * node_compat;
    // base_score = (
    //     (node_score + mem_score + stor_score) / 3.0
    // ) * node_compat * mem_compat * storage_compat * type_score
    auto final_bid = std::min(1.0, base_score * queue_factor);

    return std::trunc(final_bid * 100.0) / 100.0;
  }

  void broadcast_bid_on_job(const std::shared_ptr<wrench::S4U_Daemon>& bidder,
                            const std::shared_ptr<JobDescription>& job_description, double bid, double tie_breaker)
  {
    // Set the number of needed bids to the size of the network of job scheduling agents
    set_num_needed_bids(get_job_scheduling_agent_network_size());
    for (const auto& other_agent : get_job_scheduling_agent_network())
      other_agent->commport->dputMessage(new wrench::BidOnJobMessage(bidder, job_description, bid, tie_breaker));
  }

  std::shared_ptr<wrench::JobSchedulingAgent> determine_bid_winner(
      const std::map<std::shared_ptr<wrench::JobSchedulingAgent>, std::pair<double, double>>& all_bids) const override
  {
    // TODO Do we need to replace this by something else?
    if (all_bids.empty())
      return nullptr;

    auto max_it = std::max_element(all_bids.begin(), all_bids.end(), [](const auto& a, const auto& b) {
      if (a.second != b.second)
        return a.second < b.second; // higher value wins
      else
        return a.first < b.first; // tie-breaker: lower pointer address wins
    });

    return max_it->first;
  }
};
#endif // HEURISTIC_BIDDING_SCHEDULING_POLICY_H
