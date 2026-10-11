#ifndef CENTRALIZED_SCHEDULING_POLICY_H
#define CENTRALIZED_SCHEDULING_POLICY_H

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <fstream>
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
#include "utils/PythonRunner.h"
#include "utils/utils.h"

XBT_LOG_EXTERNAL_CATEGORY(swarm_dmas);

// Structure to hold system info for centralized decision making
struct HPCSystemInfo {
  std::shared_ptr<wrench::JobSchedulingAgent> agent;
  std::shared_ptr<HPCSystemDescription> description;
  std::shared_ptr<HPCSystemStatus> status;
};

struct CentralizedSchedulingDecision {
  std::shared_ptr<wrench::JobSchedulingAgent> target_agent;
  double decision_time;
  std::string bids;
  size_t num_top_bids; // number of systems sharing the highest bid
};

class CentralizedSchedulingPolicy {
  std::string python_script_name_;
  std::string bidder_prompt_;
  double runtime_fraction_lower_bound_;
  // CEN keeps one worker per system so each system reuses its own client and credentials.
  std::map<std::string, std::unique_ptr<PersistentPythonWorker>> python_workers_;

  PersistentPythonWorker& worker_for_system(const std::string& system_name)
  {
    auto& worker = python_workers_[system_name];
    if (!worker)
      worker = std::make_unique<PersistentPythonWorker>(python_script_name_);
    return *worker;
  }

public:
  CentralizedSchedulingPolicy(const std::string& python_script_name, const std::string& bidder_prompt_file,
                              double runtime_fraction_lower_bound)
      : python_script_name_(python_script_name), runtime_fraction_lower_bound_(runtime_fraction_lower_bound)
  {
    if (!bidder_prompt_file.empty()) {
      std::ifstream prompt_file(bidder_prompt_file);
      if (!prompt_file.is_open())
        throw std::runtime_error("Failed to open bidder prompt file: " + bidder_prompt_file);

      bidder_prompt_.assign((std::istreambuf_iterator<char>(prompt_file)), std::istreambuf_iterator<char>());
      if (bidder_prompt_.empty())
        throw std::runtime_error("Bidder prompt file is empty: " + bidder_prompt_file);
    }
  }

  // Select the best system for a job by collecting one bid from each system.
  CentralizedSchedulingDecision
  select_best_system(const std::shared_ptr<JobDescription>& job_description,
                     const std::vector<HPCSystemInfo>& systems_info)
  {
    if (systems_info.empty())
      return {nullptr, 0.0, ""};

    if (access(python_script_name_.c_str(), F_OK) != 0)
      throw std::runtime_error("Python script not found: " + python_script_name_);

    const int N = static_cast<int>(systems_info.size());
    std::vector<std::string> responses(N);

    if (uses_persistent_claude_worker(python_script_name_)) {
      std::vector<PersistentPythonWorker*> workers(N, nullptr);

      // Send to every system before waiting, preserving parallel model requests.
      for (int i = 0; i < N; i++) {
        nlohmann::json input;
        input["job_description"]        = job_description->to_json();
        input["hpc_system_description"] = systems_info[i].description->to_json();
        input["hpc_system_status"]      = systems_info[i].status->to_json();
        input["current_simulated_time"] = wrench::S4U_Simulation::getClock();
        input["runtime_fraction_lower_bound"] = runtime_fraction_lower_bound_;
        // Forward the configured prompt just as DEC does; otherwise the Claude bidder falls back.
        if (!bidder_prompt_.empty())
          input["prompt"] = bidder_prompt_;
        try {
          workers[i] = &worker_for_system(systems_info[i].description->get_name());
          workers[i]->send_request(input);
        } catch (const std::exception&) {
          workers[i] = nullptr;
        }
      }

      for (int i = 0; i < N; i++) {
        if (workers[i] == nullptr)
          continue;
        try {
          responses[i] = workers[i]->read_response().dump();
        } catch (const std::exception&) {
          responses[i].clear();
        }
      }
    } else {
      std::vector<int> read_fds(N, -1);
      std::vector<pid_t> pids(N, -1);

      for (int i = 0; i < N; i++) {
        int to_child[2], from_child[2];
        if (pipe(to_child) == -1 || pipe(from_child) == -1)
          throw std::runtime_error("Failed to create pipes");

        pids[i] = fork();
        if (pids[i] == 0) {
          dup2(to_child[0], STDIN_FILENO);
          dup2(from_child[1], STDOUT_FILENO);
          close(to_child[0]); close(to_child[1]);
          close(from_child[0]); close(from_child[1]);
          execlp("python3", "python3", python_script_name_.c_str(), nullptr);
          perror("execlp failed");
          exit(1);
        }
        close(to_child[0]);
        close(from_child[1]);

        nlohmann::json input;
        input["job_description"]        = job_description->to_json();
        input["hpc_system_description"] = systems_info[i].description->to_json();
        input["hpc_system_status"]      = systems_info[i].status->to_json();
        input["current_simulated_time"] = wrench::S4U_Simulation::getClock();
        input["runtime_fraction_lower_bound"] = runtime_fraction_lower_bound_;
        if (!bidder_prompt_.empty())
          input["prompt"] = bidder_prompt_;
        const std::string serialized_input = input.dump();
        write(to_child[1], serialized_input.c_str(), serialized_input.size());
        close(to_child[1]);
        read_fds[i] = from_child[0];
      }

      for (int i = 0; i < N; i++) {
        char buffer[256];
        ssize_t count;
        while ((count = read(read_fds[i], buffer, sizeof(buffer) - 1)) > 0) {
          buffer[count] = '\0';
          responses[i] += buffer;
        }
        close(read_fds[i]);
        waitpid(pids[i], nullptr, 0);
      }
    }

    // Use the Python-reported bid time, excluding interpreter startup from simulated decision time.
    std::map<std::shared_ptr<wrench::JobSchedulingAgent>, std::pair<double, double>> all_bids;
    constexpr uint64_t SEED  = 42;
    auto job_id_val           = static_cast<uint64_t>(job_description->get_job_id());
    double decision_time      = 0.0; // max(bid_generation_time_seconds) across all systems

    for (int i = 0; i < N; i++) {
      double bid = 0.0;
      try {
        nlohmann::json result = nlohmann::json::parse(responses[i]);
        XBT_CVERB(swarm_dmas, "Centralized bid from %s: %s",
                  systems_info[i].description->get_name().c_str(), result.dump().c_str());
        if (result.contains("bid") && result["bid"].is_number())
          bid = result["bid"].get<double>();
        if (result.contains("bid_generation_time_seconds") && result["bid_generation_time_seconds"].is_number())
          decision_time = std::max(decision_time, result["bid_generation_time_seconds"].get<double>());
      } catch (...) { /* treat parse error as bid = 0, no update to decision_time */ }

      const auto& sys_name   = systems_info[i].description->get_name();
      uint64_t mixed         = SEED ^ (job_id_val * 6364136223846793005ULL)
                                    ^ std::hash<std::string>{}(sys_name);
      std::mt19937_64 rng(mixed);
      std::uniform_real_distribution<double> dist(0.0, 100.0);
      double tie_breaker = dist(rng);
      all_bids[systems_info[i].agent] = {bid, tie_breaker};
    }

    auto bids         = get_all_bids_as_string(all_bids);
    auto num_top_bids = count_top_bids(all_bids);

    // Same comparator as PythonBiddingSchedulingPolicy::determine_bid_winner
    auto max_it = std::max_element(all_bids.begin(), all_bids.end(),
                                   [](const auto& a, const auto& b) { return a.second < b.second; });

    if (max_it->second.first <= 0.0)
      return {nullptr, decision_time, bids, num_top_bids};

    if (num_top_bids > 1)
      XBT_CINFO(swarm_dmas, "Job #%d: %zu systems share the highest bid, the tie-breaker placed it on '%s'",
                job_description->get_job_id(), num_top_bids, max_it->first->get_hpc_system_name().c_str());
    return {max_it->first, decision_time, bids, num_top_bids};
  }
};

#endif // CENTRALIZED_SCHEDULING_POLICY_H
