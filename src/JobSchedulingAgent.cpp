#include "JobSchedulingAgent.h"
#include "ControlMessages.h"
#include "HPCSystemStatus.h"
#include "JobLifecycleTrackerAgent.h"
#include "utils.h"

WRENCH_LOG_CATEGORY(job_scheduling_agent, "Log category for JobSchedulingAgent");

namespace wrench {

void JobSchedulingAgent::processEventCustom(const std::shared_ptr<CustomEvent>& event)
{
  // Receive a Job Request message. It can be an initial submission or a forward.
  if (auto job_request_message = std::dynamic_pointer_cast<JobRequestMessage>(event->message)) {
    const auto& job_description = job_request_message->get_job_description();
    WRENCH_DEBUG("Received a job request message for Job #%d: %lu compute nodes for %llu seconds",
                 job_description->get_job_id(), job_description->get_num_nodes(), job_description->get_walltime());

    // Check if this job request is an initial submission from the Workload Submission Agent that can be
    // forwarded (depending on the SchedulingPolicy) to other Job Scheduling Agents.
    if (job_request_message->can_be_forwarded()) {
      // This is an initial submission
      // Step 1: Broadcast the JobDescription to the network of Job Scheduling Agents
      scheduling_policy_->broadcast_job_description(this->getName(), job_description);
    }

    // Step 2: Retrieve current state of the HPC_system:
    // 1) number of available node
    // 2) an estimate of the start time for this particular job
    auto current_system_status =
        std::make_shared<HPCSystemStatus>(get_number_of_available_nodes_on(batch_compute_service_),
                                          get_job_start_time_estimate_on(job_description, batch_compute_service_),
                                          get_queue_length(batch_compute_service_));

    // Step 3: Compute a bid for this job description. This bid is based on
    // 1) The job description
    // 2) The HPC system description
    // 3) The current state of the HPC system
    auto local_bid = scheduling_policy_->compute_bid(job_description, hpc_system_description_, current_system_status);

    // Keep track of my bid on this job
    local_bids_[job_description->get_job_id()] = local_bid;
    WRENCH_INFO("%s computed a bid for Job #%d of %.2f", hpc_system_description_->get_cname(),
                job_description->get_job_id(), local_bid);

    // Step 4: Broadcast the local bid to the network of agents
    scheduling_policy_->broadcast_bid_on_job(shared_from_this(), job_description, local_bid);
  }

  // Receive a bid for a job
  if (auto bid_on_job_message = std::dynamic_pointer_cast<BidOnJobMessage>(event->message)) {
    auto job_description = bid_on_job_message->get_job_description();
    auto job_id          = job_description->get_job_id();
    auto remote_bidder   = bid_on_job_message->get_bidder();
    auto remote_bid      = bid_on_job_message->get_bid();
    // Increase the number of received bids fot this job
    scheduling_policy_->received_bid_for(this->getName(), job_id);
    WRENCH_DEBUG("Received a bid (%lu/%lu) for Job #%d from %s: %.2f",
                 scheduling_policy_->get_num_received_bids(this->getName(), job_id),
                 scheduling_policy_->get_num_needed_bids(), job_id, remote_bidder->get_hpc_system_name().c_str(),
                 remote_bid);

    // Store this remote bid
    all_bids_[job_id].try_emplace(remote_bidder, remote_bid);

    if (scheduling_policy_->get_num_received_bids(this->getName(), job_id) ==
        scheduling_policy_->get_num_needed_bids()) {
      // All the bids needed to take a decision in the competitive bidding process have been received
      // Step 5: Determine if this agent won the competitive bidding.
      if (this->getName() == scheduling_policy_->determine_bid_winner(all_bids_[job_id])->getName()) {
        if (do_pass_acceptance_tests(job_description, hpc_system_description_)) {
          WRENCH_INFO("Schedule Job #%d (%lu compute nodes for %llu seconds) on '%s'", job_id,
                      job_description->get_num_nodes(), job_description->get_walltime(),
                      hpc_system_description_->get_cname());
          auto job = job_manager_->createCompoundJob(std::to_string(job_id));
          job->addSleepAction("", job_description->get_walltime());
          std::map<string, string> job_args = {{"-N", std::to_string(job_description->get_num_nodes())},
                                               {"-t", std::to_string(job_description->get_walltime())},
                                               {"-c", "1"}};
          job_manager_->submitJob(job, batch_compute_service_, job_args);
        } else {
          WRENCH_INFO("Job #%d did not pass acceptance and has failed. Notifying the Job Lifecycle Tracker Agent",
               job_id);
          tracker_->commport->dputMessage(new JobNotificationMessage(std::to_string(job_id)));
        }
      } // if this agent did not win, just proceed.
    } // More bids need to be received
  }
}

void JobSchedulingAgent::processEventCompoundJobCompletion(const std::shared_ptr<CompoundJobCompletedEvent>& event)
{
  auto job_name = event->job->getName();
  WRENCH_DEBUG("Job #%s, which I ran locally, has completed. Notifying the Job Lifecycle Tracker Agent",
               job_name.c_str());
  tracker_->commport->dputMessage(new JobNotificationMessage(job_name));
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
