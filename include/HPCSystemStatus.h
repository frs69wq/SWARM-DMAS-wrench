#ifndef HPC_SYSTEM_STATUS_H
#define HPC_SYSTEM_STATUS_H

#include <cstddef>

class HPCSystemStatus {
  size_t current_num_avaibable_nodes_;
  double current_job_start_time_estimate_;

public:
  HPCSystemStatus(size_t current_num_avaibable_nodes, double current_job_start_time_estimate)
      : current_num_avaibable_nodes_(current_num_avaibable_nodes)
      , current_job_start_time_estimate_(current_job_start_time_estimate)
  {
  }

  size_t get_current_num_avaibable_nodes() const { return current_num_avaibable_nodes_; }
  double get_current_job_start_time_estimate() const { return current_job_start_time_estimate_; }
};

#endif // HPC_SYSTEM_STATUS_H