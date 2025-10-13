#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>

#include "JobDescription.h"
#include "utils.h"

using json = nlohmann::json;

std::shared_ptr<std::vector<std::shared_ptr<JobDescription>>>
extract_job_descriptions(const std::string& filename)
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
        item.at("Walltime").get<int>(), item.at("Nodes").get<int>(), item.at("RequestedGPU").get<bool>(),
        item.at("MemoryGB").get<int>(), item.at("RequestedStorageGB").get<int>(), item.at("HPCSite").get<std::string>(),
        item.at("HPCSystem").get<std::string>());

    jobs->push_back(job);
  }

  return jobs;
}
