#include "JobLifecycleTrackerAgent.h"
#include "ControlMessages.h"
#include "utils.h"

WRENCH_LOG_CATEGORY(job_lifecycle_tracker_agent, "Log category for JobLifecycleTrackerAgent");

namespace wrench {

void JobLifecycleTrackerAgent::processEventCustom(const std::shared_ptr<CustomEvent>& event)
{
  if (auto job_notification_message = std::dynamic_pointer_cast<JobNotificationMessage>(event->message)) {
    WRENCH_INFO("Notified that %s has completed!", job_notification_message->_name.c_str());
    num_completed_jobs_++;
  }
}

int JobLifecycleTrackerAgent::main()
{
  TerminalOutput::setThisProcessLoggingColor(TerminalOutput::COLOR_RED);
  WRENCH_INFO("Job Lifecycle Tracker Agent starting");
  auto jobs = extract_job_descriptions(job_list_);
  // Compute and store the total number of jobs in the workload in total_num_jobs
  size_t total_num_jobs = jobs->size();

  while (num_completed_jobs_ < total_num_jobs)
    this->waitForAndProcessNextEvent();

  return 0;
}

} // namespace wrench
