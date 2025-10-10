#ifndef HPC_SYSTEM_DESCRIPTION_H
#define HPC_SYSTEM_DESCRIPTION_H

#include <string>

enum class HPCSystemType { HPC, AI, GPU, HYBRID, MEMORY, STORAGE };

class HPCSystemDescription {
  std::string name_;
  HPCSystemType type_;
  size_t num_nodes_;
  int memory_amount_in_gb_;
  bool has_gpu_;
  int storage_amount_in_gb_;
  std::string network_interconnect_;

public:
  // Getters
  const std::string& get_name() const { return name_; }
  const char* get_cname() const { return name_.c_str(); }
  HPCSystemType get_type() const { return type_; }
  size_t get_num_nodes() const { return num_nodes_; }
  int get_memory_amount_in_gb() const { return memory_amount_in_gb_; }
  bool get_has_gpu() const { return has_gpu_; }
  int get_storage_amount_in_gb() const { return storage_amount_in_gb_; }
  const std::string& get_network_interconnect() const { return network_interconnect_; }

  // Setters
  void set_name(const std::string& name) { name_ = name; }
  void set_type(HPCSystemType type) { type_ = type; }
  void set_num_nodes(size_t num_nodes) { num_nodes_ = num_nodes; }
  void set_memory_amount_in_gb(int memory_gb) { memory_amount_in_gb_ = memory_gb; }
  void set_has_gpu(bool has_gpu) { has_gpu_ = has_gpu; }
  void set_storage_amount_in_gb(int storage_gb) { storage_amount_in_gb_ = storage_gb; }
  void set_network_interconnect(const std::string& interconnect) { network_interconnect_ = interconnect; }
};

#endif // HPC_SYSTEM_DESCRIPTION_H