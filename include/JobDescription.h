#ifndef JOB_DESCRIPTION_H
#define JOB_DESCRIPTION_H
#pragma once
#include <string>

class JobDescription {
    int job_id_;
    int submission_time_;
    int walltime_;
    int nodes_;
    int memory_gb_;
    bool requested_gpu_;
    int requested_storage_gb_;
    std::string job_type_;
    std::string user_id_;
    std::string group_id_;
    std::string hpc_site_;
    std::string hpc_system_;
    
public:
    // getters
    int get_job_id() const { return job_id_; }
    int get_submission_time() const { return submission_time_; }
    int get_walltime() const { return walltime_; }
    int get_nodes() const { return nodes_; }
    int get_memory_gb() const { return memory_gb_; }
    bool is_requested_gpu() const { return requested_gpu_; }
    int get_requested_storage_gb() const { return requested_storage_gb_; }
    const std::string& get_job_type() const { return job_type_; }
    const std::string& get_user_id() const { return user_id_; }
    const std::string& get_group_id() const { return group_id_; }
    const std::string& get_hpc_site() const { return hpc_site_; }
    const std::string& get_hpc_system() const { return hpc_system_; }

    // Setters
    void set_job_id(int value) { job_id_ = value; }
    void set_submission_time(int value) { submission_time_ = value; }
    void set_walltime(int value) { walltime_ = value; }
    void set_nodes(int value) { nodes_ = value; }
    void set_memory_gb(int value) { memory_gb_ = value; }
    void set_requested_gpu(bool value) { requested_gpu_ = value; }
    void set_requested_storage_gb(int value) { requested_storage_gb_ = value; }
    void set_job_type(const std::string& value) { job_type_ = value; }
    void set_user_id(const std::string& value) { user_id_ = value; }
    void set_group_id(const std::string& value) { group_id_ = value; }
    void set_hpc_site(const std::string& value) { hpc_site_ = value; }
    void set_hpc_system(const std::string& value) { hpc_system_ = value; }
};
#endif //JOB_DESCRIPTION_H