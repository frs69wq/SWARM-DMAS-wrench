#include <fstream>

#include "agents/ResourceSwitchingAgent.h"

WRENCH_LOG_CATEGORY(resource_switching_agent, "Log category for ResourceSwitchingAgent");

namespace wrench {

using json = nlohmann::json;

void ResourceSwitchingAgent::parse_failure_profile(const std::string& json_file)
{
  std::ifstream file(json_file);
  json j;
  file >> j;

  for (const auto& entry : j) {
    FailureEvent event;
    event.type     = entry["type"];
    event.resource = entry["resource"];
    if (entry.contains("fraction"))
      event.fraction = entry["fraction"].get<double>();
    event.turn_off_time = entry["turn_off_time"].get<double>();
    if (entry.contains("turn_on_time"))
      event.turn_on_time = entry["turn_on_time"].get<double>();

    failure_events_.push_back(event);
  }
}

void ResourceSwitchingAgent::schedule_events()
{
  for (const auto& event : failure_events_) {
    this->setTimer(event.turn_off_time, "turn_off_" + event.resource+ "_f" + std::to_string(event.fraction));
    if (event.turn_on_time > 0)
      this->setTimer(event.turn_on_time, "turn_on_" + event.resource+ "_f" + std::to_string(event.fraction));
  }
}

int ResourceSwitchingAgent::main()
{
  WRENCH_INFO("Resource Switching Agent starting");
  schedule_events();

  while (true) {
    auto event = this->waitForNextEvent();
    if (auto timer_event = std::dynamic_pointer_cast<TimerEvent>(event)) {
      const std::string& message  = timer_event->message;
      auto turn_on                = message.rfind("turn_on_", 0) == 0;
      std::string prefix          = turn_on ? "turn_on_" : "turn_off_";

      // Remove prefix
      auto rest = message.substr(prefix.length());

      // Split the rest of message on "_f"
      auto pos = rest.find("_f");
      auto resource_name = rest.substr(0, pos);
      auto fraction = std::stod(rest.substr(pos + 2));

      if (resource_name.find("link") != std::string::npos) {
        if (turn_on) {
          WRENCH_INFO("Turning Link '%s' ON", resource_name.c_str());
          // wrench::S4U_Simulation::turnOnLink(resource_name);
        } else {
          WRENCH_INFO("Turning Link '%s' OFF", resource_name.c_str());
          // wrench::S4U_Simulation::turnOffLink(resource_name);
        }
      } else { 
        auto hostnames  = clusters_[resource_name];
        auto nhosts_to_stop = static_cast<size_t>(std::ceil(hostnames.size() * fraction));
        if (turn_on) {
          WRENCH_INFO("Turning %d%% of HPC system '%s' ON", static_cast<int>(100 * fraction), resource_name.c_str());
        } else {
          WRENCH_INFO("Turning %d%% of HPC system '%s' OFF", static_cast<int>(100 * fraction), resource_name.c_str());
        }
        // for (const auto& name : hostnames) {
        //   if (turn_on)
        //     wrench::S4U_Simulation::turnOnHost(name);
        //   else
        //     wrench::S4U_Simulation::turnOffHost(name);
        // }
      } 
    }
  }

  return 0;
}

} // namespace wrench
