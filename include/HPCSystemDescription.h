#ifndef HPC_SYSTEM_DESCRIPTION_H
#define HPC_SYSTEM_DESCRIPTION_H

#include <string>

enum class HPCSystemType { HPC, AI, GPU, HYBRID, MEMORY, STORAGE };

class HPCSystemDescription {
  std::string name_;
  HPCSystemType type_;
  size_t num_nodes_;
  int memory_amount_in_gb_;
  int storage_amount_in_gb_;
  bool has_gpu_;
  std::string network_interconnect_;

public:
  HPCSystemDescription(const std::string& name, HPCSystemType type, size_t num_nodes, int memory_amount_in_gb,
                       bool has_gpu, int storage_amount_in_gb, const std::string& network_interconnect)
      : name_(name)
      , type_(type)
      , num_nodes_(num_nodes)
      , memory_amount_in_gb_(memory_amount_in_gb)
      , has_gpu_(has_gpu)
      , storage_amount_in_gb_(storage_amount_in_gb)
      , network_interconnect_(network_interconnect)
  {
  }

  // Getters
  const std::string& get_name() const { return name_; }
  const char* get_cname() const { return name_.c_str(); }
  HPCSystemType get_type() const { return type_; }
  size_t get_num_nodes() const { return num_nodes_; }
  int get_memory_amount_in_gb() const { return memory_amount_in_gb_; }
  bool get_has_gpu() const { return has_gpu_; }
  int get_storage_amount_in_gb() const { return storage_amount_in_gb_; }
  const std::string& get_network_interconnect() const { return network_interconnect_; }

  static HPCSystemType string_to_hpc_system_type(const std::string& s)
  {
    static const std::unordered_map<std::string, HPCSystemType> EnumStrings{
        {"HPC", HPCSystemType::HPC}, {"AI", HPCSystemType::AI},         {"HYBRID", HPCSystemType::HYBRID},
        {"GPU", HPCSystemType::GPU}, {"MEMORY", HPCSystemType::MEMORY}, {"STORAGE", HPCSystemType::STORAGE}};

    auto it = EnumStrings.find(s);
    if (it != EnumStrings.end()) {
      return it->second;
    } else {
      throw std::out_of_range("Invalid HPCSystemType string: " + s);
    }
  }
};

#endif // HPC_SYSTEM_DESCRIPTION_H