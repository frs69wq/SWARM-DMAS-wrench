#ifndef UTILS_H
#define UTILS_H

#include <memory>
#include <vector>
#include "utils.h"
#include "JobDescription.h"

std::shared_ptr<std::vector<std::shared_ptr<JobDescription>>>
extract_job_descriptions(const std::string& filename);

#endif // UTILS_H
