#include "agents/WorkloadCentralizedSubmissionAgent.h"
#include "agents/JobLifecycleTrackerAgent.h"
#include "agents/JobSchedulingAgent.h"
#include "info/HPCSystemStatus.h"
#include "messages/ControlMessages.h"
#include "utils/utils.h"

WRENCH_LOG_CATEGORY(workload_centralized_submission_agent, "Log category for WorkloadCentralizedSubmissionAgent");

namespace wrench {

int WorkloadCentralizedSubmissionAgent::main()
{
  TerminalOutput::setThisProcessLoggingColor(TerminalOutput::COLOR_CYAN);
  WRENCH_INFO("Workload Centralized Submission Agent starting");

  // Open and parse the JSON file that describes the entire workload
  auto jobs = extract_job_descriptions(job_list_);
  size_t total_num_jobs  = jobs->size();
  int    next_job_to_submit = 0;

  // Two-phase state: arrival fires the Python decision; dispatch fires after decision_time
  // has elapsed in simulated time, matching the decentralized per-agent overhead.
  bool awaiting_dispatch = false;
  std::shared_ptr<wrench::JobSchedulingAgent> pending_target = nullptr;

  this->setTimer(jobs->at(0)->get_submission_time(), "arrival");

  while (next_job_to_submit < total_num_jobs) {

    auto event = this->waitForNextEvent();

    if (std::dynamic_pointer_cast<TimerEvent>(event)) {

      if (!awaiting_dispatch) {
        // ── Arrival phase ──────────────────────────────────────────────────────
        // Query system statuses at arrival time and run the parallel Python decision.
        auto next_job = jobs->at(next_job_to_submit);

        std::vector<HPCSystemInfo> systems_info;
        for (const auto& agent : job_scheduling_agents_) {
          const auto& system_description = agent->get_hpc_system_description();
          const auto& batch_service      = agent->get_batch_compute_service();
          auto current_status = std::make_shared<HPCSystemStatus>(
              get_number_of_available_nodes_on(batch_service),
              get_job_start_time_estimate_on(next_job, batch_service),
              get_queue_length(batch_service));
          systems_info.push_back({agent, system_description, current_status});
        }

        auto [target_agent, decision_time] = scheduling_policy_->select_best_system(next_job, systems_info);
        pending_target   = target_agent;
        awaiting_dispatch = true;
        this->setTimer(S4U_Simulation::getClock() + decision_time, "dispatch");

      } else {
        // ── Dispatch phase ─────────────────────────────────────────────────────
        // The simulated clock has now advanced by decision_time; dispatch the job.
        auto next_job = jobs->at(next_job_to_submit);
        auto job_id   = next_job->get_job_id();

        if (pending_target == nullptr) {
          WRENCH_INFO("Job #%d cannot run on any system (all bids = 0)", job_id);
          tracker_->commport->dputMessage(
              new JobLifecycleTrackingMessage(job_id, "WorkloadCentralizedSubmissionAgent",
                                              S4U_Simulation::getClock(),
                                              JobLifecycleEventType::REJECT, "", "No feasible HPC system"));
        } else {
          auto selected_system = pending_target->get_hpc_system_name();
          WRENCH_DEBUG("Sending Job #%d to centrally-selected system '%s'", job_id, selected_system.c_str());
          pending_target->commport->dputMessage(new JobRequestMessage(next_job, false, true));
          tracker_->commport->dputMessage(new JobLifecycleTrackingMessage(
              job_id, "WorkloadCentralizedSubmissionAgent", wrench::S4U_Simulation::getClock(),
              JobLifecycleEventType::SUBMISSION, selected_system));
        }

        awaiting_dispatch = false;
        pending_target    = nullptr;
        next_job_to_submit++;
        if (next_job_to_submit < total_num_jobs)
          this->setTimer(jobs->at(next_job_to_submit)->get_submission_time(), "arrival");
      }
    }
  }
  return 0;
}

} // namespace wrench
