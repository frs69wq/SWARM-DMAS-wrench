#ifndef JOB_SCHEDULING_AGENT_H
#define JOB_SCHEDULING_AGENT_H

#include <memory>
#include <wrench-dev.h>

#include "HPCSystemDescription.h"
#include "SchedulingPolicy.h"

namespace wrench {

class JobLifecycleTrackerAgent;

/**
 *  @brief An execution controller implementation
 */
class JobSchedulingAgent : public ExecutionController {
  std::shared_ptr<HPCSystemDescription> hpc_system_description_;
  std::shared_ptr<SchedulingPolicy> scheduling_policy_;
  std::shared_ptr<JobManager> job_manager_;

  std::shared_ptr<BatchComputeService> batch_compute_service_;
  std::vector<std::shared_ptr<JobSchedulingAgent>> job_scheduling_agent_network_;
  std::shared_ptr<JobLifecycleTrackerAgent> tracker_;

  std::unordered_map<int, double> local_bids_;
  std::unordered_map<int, std::map<std::shared_ptr<JobSchedulingAgent>, double>> all_bids_;

  int main() override;
  void processEventCustom(const std::shared_ptr<CustomEvent>& event) override;
  void processEventCompoundJobCompletion(const std::shared_ptr<CompoundJobCompletedEvent>& event) override;
  void processEventCompoundJobFailure(const std::shared_ptr<CompoundJobFailedEvent>& event) override;

public:
  JobSchedulingAgent(const std::string& hostname, const std::shared_ptr<HPCSystemDescription>& hpc_system_description,
                     const std::shared_ptr<SchedulingPolicy>& scheduling_policy,
                     const std::shared_ptr<BatchComputeService>& batch_compute_service)
      : ExecutionController(hostname, "job_scheduling_agent")
      , hpc_system_description_(hpc_system_description)
      , scheduling_policy_(scheduling_policy)
      , batch_compute_service_(batch_compute_service)
  {
  }

  void add_job_scheduling_agent(std::shared_ptr<JobSchedulingAgent> agent)
  {
    job_scheduling_agent_network_.push_back(agent);
  }
  void set_job_lifecycle_tracker(std::shared_ptr<JobLifecycleTrackerAgent> tracker) { tracker_ = tracker; }
  const std::string& get_hpc_system_name() const { return hpc_system_description_->get_name(); }
};

} // namespace wrench
#endif // JOB_SCHEDULING_AGENT_H
