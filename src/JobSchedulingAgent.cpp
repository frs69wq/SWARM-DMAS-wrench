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
    auto job_description = job_request_message->get_job_description();
    WRENCH_INFO("Received a job request message for Job #%d: %d compute nodes for %d seconds",
                job_description->get_job_id(), job_description->get_num_nodes(),
                job_description->get_walltime());

    // Check if this job request is an initial submission from the Workload Submission Agent that can be
    // forwarded (depending on the SchedulingPolicy) to other Job Scheduling Agents.
    if (job_request_message->can_be_forwarded()) {
      // Step 1: Broadcast the JobDescription to the network of Job Scheduling Agents
      // scheduling_policy_->broadcast_job_description(peers_, job_description);
    }
    // FIXME Keeps what follows to have a minimal working product until something better is implemented
    // Decide whether to forward or not based on some random criterion
    if (job_request_message->can_be_forwarded() && (job_description->get_num_nodes() % 32)) {
       // Forward the job
      WRENCH_INFO("Decided to forward this job to one of my peers!");
      peers_[job_description->get_job_id()*47 % peers_.size()]->commport->dputMessage(new JobRequestMessage(
          job_description, false));
    } else {

    // Step 2: Retrieve current state of the HPC_system
    // auto system_status = TODO

    // Step 3: Compute a bid for this job description. This bid is based on
    // 1) The job description
    // 2) The HPC system description
    // 3) The current state of the HPC system
    // auto local_bid = scheduling_policy_->compute_bid(job_description, hpc_system_description_, system_status);

    // Step 4: Determine if this agent won the competitive bidding.
    // if (did_win_bid(peers_, local_bid)) {
      WRENCH_INFO("Schedule Job #%d (%d compute nodes for %d seconds) on '%s'",
                  job_description->get_job_id(), job_description->get_num_nodes(),
                  job_description->get_walltime(), hpc_system_description_->get_cname());
      auto job = job_manager_->createCompoundJob(std::to_string(job_description->get_job_id()));
      job->addSleepAction("", job_description->get_walltime());
      std::map<string, string> job_args = {
        {"-N", std::to_string(job_description->get_num_nodes())},
        {"-t", std::to_string(job_description->get_walltime())},
        {"-c", "1"}
      };
      job_manager_->submitJob(job, batch_compute_service_, job_args);
    }
    //} if this agent did not win, just proceed.
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
