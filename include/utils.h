#ifndef UTILS_H
#define UTILS_H

#include <memory>
#include <vector>
#include <wrench-dev.h>

#include "HPCSystemDescription.h"
#include "JobDescription.h"

std::shared_ptr<std::vector<std::shared_ptr<JobDescription>>> extract_job_descriptions(const std::string& filename);

size_t get_number_of_available_nodes_on(const std::shared_ptr<wrench::BatchComputeService>& batch);
double get_job_start_time_estimate_on(const std::shared_ptr<JobDescription>& job_description,
                                      const std::shared_ptr<wrench::BatchComputeService>& batch);
bool do_pass_acceptance_tests(const std::shared_ptr<JobDescription>& job_description,
                              const std::shared_ptr<HPCSystemDescription>& hpc_system_description);

#endif // UTILS_H
