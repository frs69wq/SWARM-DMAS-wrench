#ifndef JOB_LIFECYCLE_H
#define JOB_LIFECYCLE_H

#include <wrench-dev.h>

class JobLifecycle {
  // Directly from workload
  int job_id_;
  double submission_time_;
  // From job lifecycle tracking
  double scheduling_time_; // submitted to a batch system
  double start_time_;
  double completion_time_; // Corresponds to completed, failed, or rejected
  // Derived quantities
  double decision_time_;  // Scheduling time - Submission time
  double waiting_time_;   // Start time - Scheduling time
  double execution_time_; // Completion time - Start time

  std::string submitted_to_;
  std::string scheduled_on_;

  std::vector<double> bids_;

  std::string final_status_;
  std::string failure_cause_;

public:
  JobLifecycle(int job_id, const std::string& submitted_to, double submission_time)
      : job_id_(job_id), submitted_to_(submitted_to), submission_time_(submission_time)
  {
  }

  void set_scheduling_time(double time) { scheduling_time_ = time; }
  void set_start_time(double time) { start_time_ = time; }
  void set_completion_time(double time) { completion_time_ = time; }

  void set_scheduled_on(const std::string& hpc_system) { scheduled_on_ = hpc_system; }

  void set_bids(const std::vector<double>& bids) { bids_ = bids; }
  void add_bid(double bid) { bids_.push_back(bid); }

  void set_final_status(const std::string& status) { final_status_ = status; }
  void set_failure_cause(const std::string& cause) { failure_cause_ = cause; }
};

#endif // JOB_LIFECYCLE_HPP
