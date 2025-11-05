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
    // 1. Feasibility check: if the job does not pass the acceptance tests, bid is 0.0
    // TODO test storage in acceptance tests
    if (do_not_pass_acceptance_tests(job_description, hpc_system_description))
      return -1.0;

    // 2. Utilization-based scores
    auto used_nodes = hpc_system_description->get_num_nodes() - hpc_system_status->get_current_num_avaibable_nodes();
    auto node_util = used_nodes / (1.0 * hpc_system_description->get_num_nodes());
    auto node_score = 1 - node_util;

    // 3. Compatibility
    auto node_compat =
        std::min(1.0, hpc_system_status->get_current_num_avaibable_nodes() / (1.0 * job_description->get_num_nodes()));

    // 5. Queue length factor
    auto queue_factor = std::max(0.1, 1 - 0.1 * hpc_system_status->get_queue_length());

    // 6. Job-Resource compatibility factor
    double resource_factor;
    auto system_type = HPCSystemDescription::hpc_system_type_to_string(hpc_system_description->get_type());
    auto job_type    = JobDescription::job_type_to_string(job_description->get_job_type());
    if (system_type == job_type)
      resource_factor = 1.0;  // Perfect match
    else if ((job_type == "HPC" && system_type != "HPC") ||
             (job_type == "AI" && system_type != "AI") ||
             (job_type == "HYBRID" && system_type != "HYBRID"))
      resource_factor = 0.8;  // Good compatibility
    else if (job_type == "STORAGE" and system_type != "STORAGE")
        resource_factor = 0.3;  // Storage jobs prefer storage system_descriptions
    else if (system_type == "STORAGE" and job_type != "STORAGE")
        resource_factor = 0.5;  // Storage system_descriptions can handle other jobs but not optimal
    else
        resource_factor = 0.5;  // Default compatibility

    // 7. Site/System preference factor
    // Apply penalty for moving a job from its initial submission site. Higher if moved to a different site than to a
    // different system. (rationale: account for network latency and data transfer cost)
    double site_factor;
    if (job_description->get_hpc_site() == hpc_system_description->get_site()) {
      if (job_description->get_hpc_system() == hpc_system_description->get_name())
        site_factor = 1.0; // Perfect match: same site and system
      else
        site_factor = 0.9; // Same site, different system
    } else
        site_factor = 0.7; // Different sites

    // 8. Delay penalty based on estimated job start time
    auto estimated_delay = hpc_system_status-> get_current_job_start_time_estimate() - wrench::S4U_Simulation::getClock();
    // Apply penalty for longer delays - systems with longer queues get lower bids
    // Scale: 0-100 time units delay -> 1.0-0.1 multiplier (exponential decay)
    // Rationale: This penalty reduces the bid for systems that would start the job much later
    // Applies a linear penalty: Systems with longer delays get progressively lower bids
    // FIXME is exponential decay really computed here?
    auto delay_penalty = std::max(0.1, 1.0 - (estimated_delay / 100.0)); //Ensure ??? penalty of 0.1

    // 9. Combine all factors
    auto base_score =  node_score * node_compat * resource_factor * site_factor * delay_penalty;
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
