#ifndef SCHEDULING_POLICY_H
#define SCHEDULING_POLICY_H

#include <vector>
#include "HPCSystemDescription.h"
#include "JobSchedulingAgent.h"
#include "JobDescription.h"

class SchedulingPolicy {
    // job_description_broadcast_cb_;
    // local_bidding_cb_;
    // consensus_cb_;

public:
    SchedulingPolicy(/* job_description_broadcast_cb, local_bidding_cb, consensus_cb */) {}
    virtual ~SchedulingPolicy() = default;

    virtual void broadcast_job_description(const std::vector<std::shared_ptr<JobSchedulingAgent>>& peers, 
                                           const std::shared_ptr<JobDescription> job_description) = 0;
    virtual double compute_bid(const std::shared_ptr<JobDescription> job_description,
                               const std::shared_ptr<HPCSystemDescription> hpc_system_description /*, hpc_system_status */) = 0;
    virtual bool did_win_bid(const std::vector<std::shared_ptr<JobSchedulingAgent>>& peers, double local_bid) const = 0;                                     
};
#endif // SCHEDULING_POLICY_H
