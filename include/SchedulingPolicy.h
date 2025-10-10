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

public:
  virtual void broadcast_job_description(const std::vector<std::shared_ptr<wrench::JobSchedulingAgent>>& peers,
                                         const std::shared_ptr<JobDescription> job_description) = 0;
  virtual double
  compute_bid(const std::shared_ptr<JobDescription> job_description,
              const std::shared_ptr<HPCSystemDescription> hpc_system_description /*, hpc_system_status */) = 0;
  virtual bool did_win_bid(const std::vector<std::shared_ptr<wrench::JobSchedulingAgent>>& peers,
                           double local_bid) const                                                         = 0;

  static std::shared_ptr<SchedulingPolicy> create_scheduling_policy(const std::string& policy_name);
};
#endif // SCHEDULING_POLICY_H
