/**
 ** An execution controller implementation that generates job specifications and
 ** sends them to batch service controllers
 **/

#include <nlohmann/json.hpp>
#include <fstream>
#include <vector>
#include <iostream>

#include "BatchServiceController.h"
#include "ControlMessages.h"
#include "JobDescription.h"
#include "WorkloadSubmissionAgent.h"

using json = nlohmann::json;
WRENCH_LOG_CATEGORY(job_generation_controller, "Log category for JobGenerationController");

static std::shared_ptr<std::vector<JobDescription>> extract_job_descriptions(const std::string& filename)
{
  auto jobs = std::make_shared<std::vector<JobDescription>>();
  std::ifstream file(filename);

  if (!file.is_open()) {
    std::cerr << "Failed to open file: " << filename << std::endl;
    return jobs;
  }

  json j;
  file >> j;

  for (const auto& item : j) {
    JobDescription job;
    job.set_job_id(item.at("JobID").get<int>());
    job.set_submission_time(item.at("SubmissionTime").get<int>());
    job.set_walltime(item.at("Walltime").get<int>());
    job.set_nodes(item.at("Nodes").get<int>());
    job.set_memory_gb(item.at("MemoryGB").get<int>());
    job.set_requested_gpu(item.at("RequestedGPU").get<bool>());
    job.set_requested_storage_gb(item.at("RequestedStorageGB").get<int>());
    job.set_job_type(item.at("JobType").get<std::string>());
    job.set_user_id(item.at("UserID").get<std::string>());
    job.set_group_id(item.at("GroupID").get<std::string>());
    job.set_hpc_site(item.at("HPCSite").get<std::string>());
    job.set_hpc_system(item.at("HPCSystem").get<std::string>());

    jobs->push_back(job);
  }

  return jobs;
}

namespace wrench {

WorkloadSubmissionAgent::WorkloadSubmissionAgent(
    const std::string& hostname, const std::string& job_list,
    const std::vector<std::shared_ptr<BatchServiceController>>& batch_service_controllers)
    : ExecutionController(hostname, "workload_generator")
    , job_list_(job_list)
    , batch_service_controllers_(batch_service_controllers)
{
}

int WorkloadSubmissionAgent::main()
{
  /* Set the logging output to GREEN */
  TerminalOutput::setThisProcessLoggingColor(TerminalOutput::COLOR_GREEN);
  WRENCH_INFO("Job generation controller starting");

  // open and parse the JSON file that describes the entire workload
  WRENCH_INFO("Creating a set of jobs to process:");
  auto jobs = extract_job_descriptions(job_list_);
  // compute and store the total number of jobs in the workload in total_num_jobs
  size_t total_num_jobs = jobs->size();

  //  WRENCH_INFO("  - %s: arrival=%d compute_nodes=%d runtime=%d", job_name.c_str(), arrival_time, num_compute_nodes,
  //              runtime);

  /* Initialize and seed a RNG */
  std::uniform_int_distribution<int> dist(100, 1000);
  std::mt19937 rng(42);
  
  /* Main loop */
  int next_job_to_submit = 0;
  int num_completed_jobs = 0;
  
  // Set a timer for the arrival of the first job
  this->setTimer(jobs->at(0).get_submission_time(), "submit the next job");
  
  while (num_completed_jobs < total_num_jobs) {
    
    // Wait for the next event
    auto event = this->waitForNextEvent();
    
    if (std::dynamic_pointer_cast<TimerEvent>(event)) {
      // If it's a timer event, then we send the job to a randomly selected batch service controller
      
      
      auto next_job = jobs->at(next_job_to_submit);
      auto job_name = std::to_string(next_job.get_job_id());
      auto job_submission_time = next_job.get_submission_time();
      auto job_walltime = next_job.get_walltime();    
      auto job_nodes = next_job.get_nodes();
      auto job_HPCSystem = next_job.get_hpc_system();
      
      WRENCH_INFO("Sending %s to batch service controller %s", job_name.c_str(), job_HPCSystem.c_str());
      std::cout << "Sending "<< job_name.c_str() << " to batch service controller " << job_HPCSystem.c_str() << std::endl;

      auto target_batch_service_controller = 
         *(std::find_if(batch_service_controllers_.begin(), batch_service_controllers_.end(),
                        [job_HPCSystem](std::shared_ptr<wrench::BatchServiceController> c) {
                          return c->get_sitename() == job_HPCSystem;
                        })
          );
      // TODO The last parameter of JobRequestMessage is whether or not the Job can be forwarded to another
      // scheduling agent. Make that a command line parameter to switch between scenarios without recompiling
      target_batch_service_controller->commport->dputMessage(
          new JobRequestMessage(job_name, job_nodes, job_walltime, true));
      
      next_job_to_submit++;

      // Set the timer for the next job, if need be
      if (next_job_to_submit < total_num_jobs) {
        auto next_job_arrival_time = jobs->at(next_job_to_submit).get_submission_time();
        this->setTimer(next_job_arrival_time, "submit the next job");
      }

    } else if (auto custom_event = std::dynamic_pointer_cast<CustomEvent>(event)) {
      // If it's a job completion notification, then we just take it into account
      if (auto job_notification_message = std::dynamic_pointer_cast<JobNotificationMessage>(custom_event->message)) {
        WRENCH_INFO("Notified that %s has completed!", job_notification_message->_name.c_str());
        num_completed_jobs++;
      }
    }
  }
  WRENCH_INFO("Terminating!");
  return 0;
}

void WorkloadSubmissionAgent::processEventCustom(const std::shared_ptr<CustomEvent>& event) 
{
  // TODO to be implemented
}
} // namespace wrench
