#ifndef JOB_DESCRIPTION_H
#define JOB_DESCRIPTION_H

#include <stdexcept>
#include <string>
#include <unordered_map>

enum class JobType { HPC, AI, HYBRID, GPU, MEMORY, STORAGE };

class JobDescription {
  int job_id_;
  int user_id_;
  int group_id_;
  JobType job_type_;
  int submission_time_;
  int walltime_;
  int num_nodes_;
  bool needs_gpu_;
  int requested_memory_gb_;
  double requested_storage_gb_;
  std::string hpc_site_;
  std::string hpc_system_;

public:
  JobDescription(int job_id, int user_id, int group_id, JobType job_type, int submission_time, int walltime,
                 int num_nodes, bool needs_gpu, int requested_memory_gb, double requested_storage_gb,
                 const std::string& hpc_site, const std::string& hpc_system)
      : job_id_(job_id)
      , user_id_(user_id)
      , group_id_(group_id)
      , job_type_(job_type)
      , submission_time_(submission_time)
      , walltime_(walltime)
      , num_nodes_(num_nodes)
      , needs_gpu_(needs_gpu)
      , requested_memory_gb_(requested_memory_gb)
      , requested_storage_gb_(requested_storage_gb)
      , hpc_site_(hpc_site)
      , hpc_system_(hpc_system)
  {
  }

  // getters
  int get_job_id() const { return job_id_; }
  int get_user_id() const { return user_id_; }
  int get_group_id() const { return group_id_; }
  JobType get_job_type() const { return job_type_; }
  int get_submission_time() const { return submission_time_; }
  int get_walltime() const { return walltime_; }
  int get_num_nodes() const { return num_nodes_; }
  bool needs_gpu() const { return needs_gpu_; }
  int get_requested_memory_gb() const { return requested_memory_gb_; }
  int get_requested_storage_gb() const { return requested_storage_gb_; }
  const std::string& get_hpc_site() const { return hpc_site_; }
  const std::string& get_hpc_system() const { return hpc_system_; }

  static JobType string_to_job_type(const std::string& s)
  {
    static const std::unordered_map<std::string, JobType> EnumStrings{
        {"HPC", JobType::HPC}, {"AI", JobType::AI},         {"HYBRID", JobType::HYBRID},
        {"GPU", JobType::GPU}, {"MEMORY", JobType::MEMORY}, {"STORAGE", JobType::STORAGE}};

    auto it = EnumStrings.find(s);
    if (it != EnumStrings.end()) {
      return it->second;
    } else {
      throw std::out_of_range("Invalid JobType string: " + s);
    }
  }
};
#endif // JOB_DESCRIPTION_H