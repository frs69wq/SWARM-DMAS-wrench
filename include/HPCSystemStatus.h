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
};

#endif // HPC_SYSTEM_STATUS_H