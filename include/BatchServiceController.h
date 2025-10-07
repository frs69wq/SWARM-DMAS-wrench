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
  BatchServiceController(const std::string& sitename,const std::string& hostname,
                         const std::shared_ptr<BatchComputeService>& batch_compute_service);

  void add_peer(std::shared_ptr<BatchServiceController> peer) { peers_.push_back(peer); }
  void setJobOriginator(std::shared_ptr<WorkloadSubmissionAgent> originator) { this->_originator = originator; }
  const std::string& get_sitename() const { return sitename_; }

private:
  int main() override;

  std::string sitename_;

  std::shared_ptr<JobManager> _job_manager;

  const std::shared_ptr<BatchComputeService> _batch_compute_service;
  std::vector<std::shared_ptr<BatchServiceController>> peers_;
  std::shared_ptr<WorkloadSubmissionAgent> _originator;

  void processEventCustom(const std::shared_ptr<CustomEvent>& event) override;
  void processEventCompoundJobCompletion(const std::shared_ptr<CompoundJobCompletedEvent>& event) override;
};

} // namespace wrench
#endif // BATCH_SERVICE_CONTROLLER_H
