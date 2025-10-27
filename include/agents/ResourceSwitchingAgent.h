#ifndef RESOURCE_SWITCHING_AGENT_H
#define RESOURCE_SWITCHING_AGENT_H

#include <nlohmann/json.hpp>
#include <simgrid/s4u.hpp>
#include <wrench-dev.h>

namespace wrench {

class ResourceSwitchingAgent : public ExecutionController {
private:
  std::map<std::string, std::vector<std::string>> clusters_;

  struct FailureEvent {
    std::string type;
    std::string resource;
    double fraction = 1.0;
    double turn_off_time;
    double turn_on_time = -1.0;
  };

  std::vector<FailureEvent> failure_events_;

  void parse_failure_profile(const std::string& json_file);
  void schedule_events();
  int main() override;

public:
  ResourceSwitchingAgent(const std::string& hostname,
                        const std::map<std::string, std::vector<std::string>>& clusters,
                        const std::string& failure_profile)
   : ExecutionController(hostname, "resource_switching_agent"), clusters_(clusters)
  {
    parse_failure_profile(failure_profile);
  }
};

} // namespace wrench

#endif // RESOURCE_SWITCHING_AGENT_H
