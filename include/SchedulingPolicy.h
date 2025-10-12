#ifndef SCHEDULING_POLICY_H
#define SCHEDULING_POLICY_H

#include "HPCSystemDescription.h"
#include "JobDescription.h"
#include <memory>
#include <vector>

namespace wrench {
class JobSchedulingAgent;
}

class SchedulingPolicy {
  size_t num_needed_bids_;
  std::unordered_map<int, size_t> num_received_bids_;
  std::vector<std::shared_ptr<wrench::JobSchedulingAgent>> job_scheduling_agent_network;

protected:
  void set_num_needed_bids(size_t value) { num_needed_bids_ = value; }
  void init_num_received_bids(int job_id) { num_received_bids_[job_id] = 0; }
  const std::vector<std::shared_ptr<wrench::JobSchedulingAgent>>& get_job_scheduling_agent_network() { return job_scheduling_agent_network; }
  size_t get_job_scheduling_agent_network_size() const { return job_scheduling_agent_network.size(); }
public:
  void set_job_scheduling_agent_network(const std::vector<std::shared_ptr<wrench::JobSchedulingAgent>>& network) { job_scheduling_agent_network = network; }

  virtual void broadcast_job_description(wrench::JobSchedulingAgent* self,
                                         const std::shared_ptr<JobDescription> job_description) = 0;
  virtual double
  compute_bid(const std::shared_ptr<JobDescription> job_description,
              const std::shared_ptr<HPCSystemDescription> hpc_system_description /*, hpc_system_status */) = 0;

  virtual void broadcast_bid_on_job(wrench::JobSchedulingAgent* bidder,
                                     const std::shared_ptr<JobDescription> job_description, double bid) = 0;

  virtual bool did_win_bid(double local_bid, const std::map<wrench::JobSchedulingAgent*, double>& remote_bids) const = 0;

  static std::shared_ptr<SchedulingPolicy> create_scheduling_policy(const std::string& policy_name);
  size_t get_num_needed_bids() const { return num_needed_bids_; }
  size_t get_num_received_bids(int job_id) const { return num_received_bids_.at(job_id); }
  void received_bid_for(int job_id) { num_received_bids_[job_id] += 1; }
};
#endif // SCHEDULING_POLICY_H
