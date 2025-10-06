#ifndef BATCH_SERVICE_CONTROLLER_H
#define BATCH_SERVICE_CONTROLLER_H

#include <wrench-dev.h>

namespace wrench {

class WorkloadSubmissionAgent;

/**
 *  @brief An execution controller implementation
 */
class BatchServiceController : public ExecutionController {

public:
  // Constructor
  BatchServiceController(const std::string& hostname,
                         const std::shared_ptr<BatchComputeService>& batch_compute_service);

  void setPeer(std::shared_ptr<BatchServiceController> peer) { this->_peer = peer; }
  void setJobOriginator(std::shared_ptr<WorkloadSubmissionAgent> originator) { this->_originator = originator; }

private:
  int main() override;

  std::shared_ptr<JobManager> _job_manager;

  const std::shared_ptr<BatchComputeService> _batch_compute_service;
  std::shared_ptr<BatchServiceController> _peer;
  std::shared_ptr<WorkloadSubmissionAgent> _originator;

  void processEventCustom(const std::shared_ptr<CustomEvent>& event) override;
  void processEventCompoundJobCompletion(const std::shared_ptr<CompoundJobCompletedEvent>& event) override;
};

} // namespace wrench
#endif // WRENCH_EXAMPLE_JOB_GENERATION_CONTROLLER_H
