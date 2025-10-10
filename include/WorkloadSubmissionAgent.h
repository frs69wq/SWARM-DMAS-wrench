#ifndef WORKLOAD_SUBMISSION_AGENT_H
#define WORKLOAD_SUBMISSION_AGENT_H

#include <wrench-dev.h>

namespace wrench {

class JobSchedulingAgent;

/**
 *  @brief An execution controller implementation
 */
class WorkloadSubmissionAgent : public ExecutionController {
  const std::string& job_list_;
  std::vector<std::shared_ptr<JobSchedulingAgent>> job_scheduling_agents_;

  int main() override;
  void processEventCustom(const std::shared_ptr<CustomEvent>& event) override { /* no-op*/ };

public:
  // Constructor
  WorkloadSubmissionAgent(const std::string& hostname, const std::string& job_list,
                          const std::vector<std::shared_ptr<JobSchedulingAgent>>& job_scheduling_agents)
      : ExecutionController(hostname, "workload_submission_agent")
      , job_list_(job_list)
      , job_scheduling_agents_(job_scheduling_agents)
  {
  }
};

} // namespace wrench
#endif // WORKLOAD_SUBMISSION_AGENT_HPP
