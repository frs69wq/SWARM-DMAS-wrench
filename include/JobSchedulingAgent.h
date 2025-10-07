#ifndef JOB_SCHEDULING_AGENT_H
#define JOB_SCHEDULING_AGENT_H

#include <wrench-dev.h>

namespace wrench {

class WorkloadSubmissionAgent;

/**
 *  @brief An execution controller implementation
 */
class JobSchedulingAgent : public ExecutionController {
  std::string sitename_;
  std::shared_ptr<JobManager> job_manager_;
  
  const std::shared_ptr<BatchComputeService> batch_compute_service_;
  std::vector<std::shared_ptr<JobSchedulingAgent>> peers_;
  std::shared_ptr<WorkloadSubmissionAgent> originator_;
  
  int main() override;
  void processEventCustom(const std::shared_ptr<CustomEvent>& event) override;
  void processEventCompoundJobCompletion(const std::shared_ptr<CompoundJobCompletedEvent>& event) override;

public:
  JobSchedulingAgent(const std::string& sitename,const std::string& hostname,
                         const std::shared_ptr<BatchComputeService>& batch_compute_service)
    : ExecutionController(hostname, "job_scheduling_agent"), sitename_(sitename), batch_compute_service_(batch_compute_service){}

  void add_peer(std::shared_ptr<JobSchedulingAgent> peer) { peers_.push_back(peer); }
  void setJobOriginator(std::shared_ptr<WorkloadSubmissionAgent> originator) { this->originator_ = originator; }
  const std::string& get_sitename() const { return sitename_; }
};

} // namespace wrench
#endif // JOB_SCHEDULING_AGENT_H
