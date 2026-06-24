#ifndef CENTRALIZED_SCHEDULING_POLICY_H
#define CENTRALIZED_SCHEDULING_POLICY_H

#include <algorithm>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <nlohmann/json.hpp>
#include <random>
#include <stdexcept>
#include <string>
#include <sys/wait.h>
#include <unistd.h>
#include <utility>
#include <vector>
#include <wrench.h>
#include <xbt/log.h>

#include "agents/JobSchedulingAgent.h"
#include "info/HPCSystemDescription.h"
#include "info/HPCSystemStatus.h"
#include "info/JobDescription.h"

XBT_LOG_EXTERNAL_CATEGORY(swarm_dmas);

// Structure to hold system info for centralized decision making
struct HPCSystemInfo {
  std::shared_ptr<wrench::JobSchedulingAgent> agent;
  std::shared_ptr<HPCSystemDescription> description;
  std::shared_ptr<HPCSystemStatus> status;
};

class CentralizedSchedulingPolicy {
  std::string python_script_name_;

public:
  explicit CentralizedSchedulingPolicy(const std::string& python_script_name) : python_script_name_(python_script_name)
  {
  }

  // Select the best system for a job given all available systems' info
  // Returns nullptr if no system can run the job
  std::shared_ptr<wrench::JobSchedulingAgent> select_best_system(const std::shared_ptr<JobDescription>& job_description,
                                                                 const std::vector<HPCSystemInfo>& systems_info)
  {
    if (systems_info.empty())
      return nullptr;

    int to_python[2];
    int from_python[2];

    if (pipe(to_python) == -1 || pipe(from_python) == -1) {
      throw std::runtime_error("Failed to create pipes");
    }

    if (access(python_script_name_.c_str(), F_OK) != 0)
      throw std::runtime_error("Python script not found: " + python_script_name_);

    pid_t pid = fork();
    if (pid == 0) {
      // External Python process
      dup2(to_python[0], STDIN_FILENO);
      dup2(from_python[1], STDOUT_FILENO);

      close(to_python[1]);
      close(from_python[0]);
      close(to_python[0]);
      close(from_python[1]);

      execlp("python3", "python3", python_script_name_.c_str(), nullptr);
      perror("execlp failed");
      exit(1);
    } else {
      // C++ process
      close(to_python[0]);
      close(from_python[1]);

      // Serialize input to JSON
      nlohmann::json j;
      j["job_description"]        = job_description->to_json();
      j["current_simulated_time"] = wrench::S4U_Simulation::getClock();

      // Build array of all systems
      nlohmann::json systems_array = nlohmann::json::array();
      for (const auto& sys_info : systems_info) {
        nlohmann::json sys_json;
        sys_json["system_name"] = sys_info.description->get_name();
        sys_json["description"] = sys_info.description->to_json();
        sys_json["status"]      = sys_info.status->to_json();
        systems_array.push_back(sys_json);
      }
      j["systems"] = systems_array;

      std::string jsonStr = j.dump();
      write(to_python[1], jsonStr.c_str(), jsonStr.size());
      close(to_python[1]);

      // Read response from python
      std::string response;
      char buffer[256];
      ssize_t count;
      while ((count = read(from_python[0], buffer, sizeof(buffer) - 1)) > 0) {
        buffer[count] = '\0';
        response += buffer;
      }
      close(from_python[0]);
      waitpid(pid, nullptr, 0);

      try {
        nlohmann::json result = nlohmann::json::parse(response);
        XBT_CVERB(swarm_dmas, "Centralized policy result: %s", result.dump().c_str());

        if (!result.contains("bids") || !result["bids"].is_object())
          throw std::runtime_error("Invalid response: 'bids' not found or not an object");

        // Build the same all_bids structure used by the decentralized determine_bid_winner,
        // generating tie-breakers with the identical seed+job_id+system_name formula so that
        // both schedulers resolve ties the same way given the same seed.
        constexpr uint64_t SEED = 42;
        auto job_id_val         = static_cast<uint64_t>(job_description->get_job_id());
        std::map<std::shared_ptr<wrench::JobSchedulingAgent>, std::pair<double, double>> all_bids;
        for (const auto& sys_info : systems_info) {
          const auto& sys_name = sys_info.description->get_name();
          double bid           = result["bids"].contains(sys_name) ? result["bids"][sys_name].get<double>() : 0.0;
          uint64_t mixed       = SEED ^ (job_id_val * 6364136223846793005ULL)
                                      ^ std::hash<std::string>{}(sys_name);
          double tie_breaker   = std::uniform_real_distribution<double>(0.0, 100.0)(std::mt19937_64(mixed));
          all_bids[sys_info.agent] = {bid, tie_breaker};
        }

        // Same comparator as PythonBiddingSchedulingPolicy::determine_bid_winner
        auto max_it = std::max_element(all_bids.begin(), all_bids.end(), [](const auto& a, const auto& b) {
          return a.second < b.second;
        });

        if (max_it->second.first <= 0.0)
          return nullptr;

        return max_it->first;
      } catch (const std::exception& e) {
        throw std::runtime_error(std::string("Failed to parse Python response: ") + e.what());
      }
    }
  }
};

#endif // CENTRALIZED_SCHEDULING_POLICY_H