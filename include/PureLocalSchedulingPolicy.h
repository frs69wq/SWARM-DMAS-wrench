#ifndef PURE_LOCAL_SCHEDULING_POLICY_H
#define PURE_LOCAL_SCHEDULING_POLICY_H

#include "SchedulingPolicy.h"

class PureLocalSchedulingPolicy : public SchedulingPolicy {
 
public:
  PureLocalSchedulingPolicy(/* job_description_broadcast_cb, local_bidding_cb, consensus_cb */) {}
  void broadcast_job_description(const std::vector<std::shared_ptr<JobSchedulingAgent>>& peers, 
                                           const std::shared_ptr<JobDescription> job_description) override {}
  double compute_bid(const std::shared_ptr<JobDescription> job_description,
                               const std::shared_ptr<HPCSystemDescription> hpc_system_description /*, hpc_system_status */) override { return 1.0; }  
  bool did_win_bid(const std::vector<std::shared_ptr<JobSchedulingAgent>>& /* peers */, double /*local_bid*/) const override 
  { 
    return true;
 }

};
#endif // PURE_LOCAL_SCHEDULING_POLICY_H
