#include "JobLifecycleTrackerAgent.h"
#include "ControlMessages.h"
#include "utils.h"

WRENCH_LOG_CATEGORY(job_lifecycle_tracker_agent, "Log category for JobLifecycleTrackerAgent");

namespace wrench {

void JobLifecycleTrackerAgent::processEventCustom(const std::shared_ptr<CustomEvent>& event)
{
  if (auto message = std::dynamic_pointer_cast<JobLifecycleTrackingMessage>(event->message)) {
    auto event_type = message->get_event_type();
    switch(event_type) {
      case JobLifecycleEventType::SUBMISSION:
        WRENCH_INFO("Job #%s has been submitted!", message->get_job_cname());
        break;
      case JobLifecycleEventType::SCHEDULING:
        WRENCH_INFO("Job #%s has been scheduled!", message->get_job_cname());
        break;
      case JobLifecycleEventType::COMPLETION:
        WRENCH_INFO("Job #%s has completed!", message->get_job_cname());
        num_completed_jobs_++;
        break;
      case JobLifecycleEventType::REJECT:
        WRENCH_INFO("Job #%s was rejected!", message->get_job_cname());
        num_rejected_jobs_++;
        break;
      default:
        throw std::invalid_argument("Unknown job lifecycle event type");
    }
  }
}

int JobLifecycleTrackerAgent::main()
{
  TerminalOutput::setThisProcessLoggingColor(TerminalOutput::COLOR_RED);
  WRENCH_INFO("Job Lifecycle Tracker Agent starting");
  auto jobs = extract_job_descriptions(job_list_);
  // Compute and store the total number of jobs in the workload in total_num_jobs
  size_t total_num_jobs = jobs->size();

  while (num_completed_jobs_ + num_rejected_jobs_ < total_num_jobs)
    this->waitForAndProcessNextEvent();

  WRENCH_INFO("Summary: %d Completed / %d Rejected jobs", num_completed_jobs_, num_rejected_jobs_);
  return 0;
}

} // namespace wrench
