#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>

#include "HPCSystemDescription.h"
#include "JobDescription.h"
#include "utils.h"

using json = nlohmann::json;

std::shared_ptr<std::vector<std::shared_ptr<JobDescription>>> extract_job_descriptions(const std::string& filename)
{
  auto jobs = std::make_shared<std::vector<std::shared_ptr<JobDescription>>>();
  std::ifstream file(filename);

  if (!file.is_open()) {
    std::cerr << "Failed to open file: " << filename << std::endl;
    return jobs;
  }

  json j;
  file >> j;

  for (const auto& item : j) {
    // FIXME use a proper ctor to fill the job description
    auto job = std::make_shared<JobDescription>(
        item.at("JobID").get<int>(), item.at("UserID").get<int>(), item.at("GroupID").get<int>(),
        JobDescription::string_to_job_type(item.at("JobType").get<std::string>()), item.at("SubmissionTime").get<int>(),
        item.at("Walltime").get<sg_size_t>(), item.at("Nodes").get<size_t>(), item.at("RequestedGPU").get<bool>(),
        item.at("MemoryGB").get<int>(), item.at("RequestedStorageGB").get<int>(), item.at("HPCSite").get<std::string>(),
        item.at("HPCSystem").get<std::string>());

    jobs->push_back(job);
  }

  return jobs;
}

size_t get_number_of_available_nodes_on(const std::shared_ptr<wrench::BatchComputeService>& batch)
{
  size_t total_number_of_available_nodes = 0;
  auto num_idle_core_map                 = batch->getPerHostNumIdleCores(false);
  // all nodes/hosts are declare as having only 1 core. Simple sum will do
  for (auto const& [host, idle_cores] : num_idle_core_map)
    total_number_of_available_nodes += idle_cores;
  return total_number_of_available_nodes;
}

double get_job_start_time_estimate_on(const std::shared_ptr<JobDescription>& job_description,
                                      const std::shared_ptr<wrench::BatchComputeService>& batch)
{
  // Fake a Wrench job to get an estimate of the job starting time on that batch compute service
  std::tuple<std::string, unsigned long, unsigned long, sg_size_t> wrench_job_description = {
      std::to_string(job_description->get_job_id()), job_description->get_num_nodes(), 1 /*num_cores*/,
      job_description->get_walltime()};
  auto current_job_start_time_estimate = batch->getStartTimeEstimates({wrench_job_description});
  return current_job_start_time_estimate.begin()->second;
}

size_t get_queue_length(const std::shared_ptr<wrench::BatchComputeService>& batch)
{
  return batch->getQueue().size();
} 

bool do_pass_acceptance_tests(const std::shared_ptr<JobDescription>& job_description,
                              const std::shared_ptr<HPCSystemDescription>& hpc_system_description)
{
  bool do_pass = true;
  if (job_description->needs_gpu() && not hpc_system_description->has_gpu())
    do_pass = false;
  if (job_description->get_num_nodes() > hpc_system_description->get_num_nodes())
    do_pass = false;
  if (job_description->get_requested_memory_gb() > 
      (hpc_system_description->get_num_nodes() * hpc_system_description->get_memory_amount_in_gb()))
    do_pass = false;
  // TODO add test for storage

  return do_pass;
}