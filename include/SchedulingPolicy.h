#ifndef SCHEDULING_POLICY_H
#define SCHEDULING_POLICY_H

#include "HPCSystemDescription.h"
#include "HPCSystemStatus.h"
#include "JobDescription.h"
#include <memory>
#include <vector>

namespace wrench {
class JobSchedulingAgent;
}

class SchedulingPolicy {
  size_t num_needed_bids_;
  std::unordered_map<int, std::unordered_map<std::string, size_t>> num_received_bids_;
  std::vector<std::shared_ptr<wrench::JobSchedulingAgent>> job_scheduling_agent_network_;

protected:
  void set_num_needed_bids(size_t value) { num_needed_bids_ = value; }

  void init_num_received_bids(int job_id);

  const std::vector<std::shared_ptr<wrench::JobSchedulingAgent>>& get_job_scheduling_agent_network()
  {
    return job_scheduling_agent_network_;
  }
  size_t get_job_scheduling_agent_network_size() const { return job_scheduling_agent_network_.size(); }

public:
  void set_job_scheduling_agent_network(const std::vector<std::shared_ptr<wrench::JobSchedulingAgent>>& network)
  {
    job_scheduling_agent_network_ = network;
  }

  virtual void broadcast_job_description(const std::string& agent_name,
                                         const std::shared_ptr<JobDescription>& job_description) = 0;
  virtual double compute_bid(const std::shared_ptr<JobDescription>& job_description,
                             const std::shared_ptr<HPCSystemDescription>& hpc_system_description,
                             const std::shared_ptr<HPCSystemStatus>& hpc_system_status)          = 0;

  virtual void broadcast_bid_on_job(const std::shared_ptr<wrench::S4U_Daemon>& bidder,
                                    const std::shared_ptr<JobDescription>& job_description, double bid) = 0;

  virtual std::shared_ptr<wrench::JobSchedulingAgent>
  determine_bid_winner(const std::map<std::shared_ptr<wrench::JobSchedulingAgent>, double>& all_bids) const = 0;

  static std::shared_ptr<SchedulingPolicy> create_scheduling_policy(const std::string& policy_name);
  size_t get_num_needed_bids() const { return num_needed_bids_; }
  size_t get_num_received_bids(const std::string& agent_name, int job_id) const
  {
    return num_received_bids_.at(job_id).at(agent_name);
  }
  void received_bid_for(const std::string& agent_name, int job_id) { num_received_bids_[job_id][agent_name] += 1; }
};
#endif // SCHEDULING_POLICY_H
