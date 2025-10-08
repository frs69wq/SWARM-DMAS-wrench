/**
 ** An batch service controller implementation that submits jobs to a batch compute
 ** service or delegate jobs to a peer...
 **/

#include "JobSchedulingAgent.h"
#include "WorkloadSubmissionAgent.h"

#include "ControlMessages.h"

WRENCH_LOG_CATEGORY(job_scheduling_agent, "Log category for JobSchedulingAgent");

namespace wrench {

void JobSchedulingAgent::processEventCustom(const std::shared_ptr<CustomEvent>& event)
{
  if (auto job_request_message = std::dynamic_pointer_cast<JobRequestMessage>(event->message)) {
    WRENCH_INFO("Received a job request message for Job #%s: %d compute nodes for %d seconds",
                job_request_message->_name.c_str(), job_request_message->_num_compute_nodes,
                job_request_message->_runtime);
    // Decide whether to forward or not based on some random criterion
    if (job_request_message->_can_forward && (job_request_message->_num_compute_nodes % 32)) {
       // Forward the job
      WRENCH_INFO("Decided to forward this job to one of my peers!");
      this->peers_[std::stoi(job_request_message->_name)*47 % peers_.size()]->commport->dputMessage(new JobRequestMessage(
          job_request_message->_name, job_request_message->_num_compute_nodes, job_request_message->_runtime, false));
    } else {
      //Do the job myself
      WRENCH_INFO("Doing this job myself!");
      auto job = job_manager_->createCompoundJob(job_request_message->_name);
      job->addSleepAction("", job_request_message->_runtime);
      // FIXME this second sleep is certainly useless 
      job->addSleepAction("", job_request_message->_runtime);
      std::map<string, string> job_args;
      job_args["-N"] = std::to_string(job_request_message->_num_compute_nodes);
      job_args["-t"] = std::to_string(job_request_message->_runtime);
      job_args["-c"] = "1";
      job_manager_->submitJob(job, batch_compute_service_, job_args);
    }
  }
}

void JobSchedulingAgent::processEventCompoundJobCompletion(const std::shared_ptr<CompoundJobCompletedEvent>& event)
{
  auto job_name = event->job->getName();
  WRENCH_INFO("Job #%s, which I ran locally, has completed. Notifying the Workload Submission Agent", job_name.c_str());
  originator_->commport->dputMessage(new JobNotificationMessage(job_name));
}

int JobSchedulingAgent::main()
{
  TerminalOutput::setThisProcessLoggingColor(TerminalOutput::COLOR_CYAN);
  WRENCH_INFO("Job Scheduling Agent starting");

  // Create my job manager
  job_manager_ = this->createJobManager();

  // Just waits for events to happen
  while (true)
    this->waitForAndProcessNextEvent();
}

} // namespace wrench
