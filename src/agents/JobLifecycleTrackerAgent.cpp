#include "agents/JobLifecycleTrackerAgent.h"
#include "messages/ControlMessages.h"
#include "utils/utils.h"

WRENCH_LOG_CATEGORY(job_lifecycle_tracker_agent, "Log category for JobLifecycleTrackerAgent");

namespace wrench {

void JobLifecycleTrackerAgent::processEventCustom(const std::shared_ptr<CustomEvent>& event)
{
  if (auto message = std::dynamic_pointer_cast<JobLifecycleTrackingMessage>(event->message)) {
    auto job_id     = message->get_job_id();
    auto pos        = job_id - 1; // jobs are numbered from 1, while the first lifecycle is .at(0), hence shifting.
    auto event_type = message->get_event_type();
    auto when       = message->get_when();
    switch (event_type) {
      case JobLifecycleEventType::SUBMISSION:
        WRENCH_INFO("Job #%d has been submitted!", job_id);
        break;
      case JobLifecycleEventType::SCHEDULING:
        WRENCH_INFO("Job #%d has been scheduled!", job_id);
        job_lifecycles_->at(pos)->set_scheduling_time(when);
        // TODO add set_scheduled_on();
        break;
      case JobLifecycleEventType::REJECT:
        WRENCH_INFO("Job #%d was rejected!", job_id);
        job_lifecycles_->at(pos)->set_reject_time(when);
        num_rejected_jobs_++;
        break;
      case JobLifecycleEventType::START:
        WRENCH_INFO("Job #%d has started!", job_id);
        job_lifecycles_->at(pos)->set_start_time(when);
        break;
      case JobLifecycleEventType::COMPLETION:
        WRENCH_INFO("Job #%d has completed!", job_id);
        job_lifecycles_->at(pos)->set_end_time(when);
        num_completed_jobs_++;
        break;
      case JobLifecycleEventType::FAIL:
        WRENCH_INFO("Job #%d has completed!", job_id);
        job_lifecycles_->at(pos)->set_end_time(when);
        num_failed_jobs_++;
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
  job_lifecycles_ = create_job_lifecycles(job_list_);
  // Compute and store the total number of jobs in the workload in total_num_jobs
  size_t total_num_jobs = job_lifecycles_->size();

  while (num_completed_jobs_ + num_rejected_jobs_ + num_failed_jobs_ < total_num_jobs)
    this->waitForAndProcessNextEvent();

  WRENCH_INFO("Summary: %d Completed / %d Failed / %d Rejected jobs", num_completed_jobs_, num_failed_jobs_,
              num_rejected_jobs_);
  return 0;
}

} // namespace wrench
