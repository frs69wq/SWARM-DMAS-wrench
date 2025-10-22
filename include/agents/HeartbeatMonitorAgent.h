#ifndef HEARTBEAT_MONITOR_AGENT_H
#define HEARTBEAT_MONITOR_AGENT_H

#include <wrench-dev.h>

namespace wrench {

class JobSchedulingAgent;

class HeartbeatMonitorAgent : public ExecutionController {
  std::shared_ptr<JobSchedulingAgent> job_scheduling_agent_;
  double period_;
  double expiration_;
  std::vector<std::shared_ptr<HeartbeatMonitorAgent>> heartbeat_monitor_agent_network_;
  std::map<std::shared_ptr<HeartbeatMonitorAgent>, double> last_heartbeat_time_;

  void send_heartbeats();
  void check_expired_heartbeats();

  int main() override;
  void processEventCustom(const std::shared_ptr<CustomEvent>& event) override { /* no-op */ };

public:
  HeartbeatMonitorAgent(const std::string& hostname, const std::shared_ptr<JobSchedulingAgent>& job_scheduling_agent,
                        double period, double expiration)
      : ExecutionController(hostname, "heartbeat_monitor_agent")
      , job_scheduling_agent_(job_scheduling_agent)
      , period_(period)
      , expiration_(expiration)
  {
  }
  void add_heartbeat_monitor_agent(std::shared_ptr<HeartbeatMonitorAgent> agent)
  {
    heartbeat_monitor_agent_network_.push_back(agent);
    last_heartbeat_time_[agent] = 0.0;
  }
};

} // namespace wrench
#endif // HEARTBEAT_MONITOR_AGENT_H
