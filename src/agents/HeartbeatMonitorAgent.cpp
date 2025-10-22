#include "agents/HeartbeatMonitorAgent.h"
#include "messages/ControlMessages.h"

WRENCH_LOG_CATEGORY(heartbeat_monitor_agent, "Log category for HeartbeatMonitorAgent");

namespace wrench {

void HeartbeatMonitorAgent::send_heartbeats()
{
  for (const auto& agent : heartbeat_monitor_agent_network_) {
    agent->commport->dputMessage(new HeartbeatMessage(shared_from_this()));
    WRENCH_DEBUG("Sent heartbeat to %s", agent->getName().c_str());
  }
}

void HeartbeatMonitorAgent::check_expired_heartbeats()
{
  double now = S4U_Simulation::getClock();
  for (const auto& [agent, last_time] : last_heartbeat_time_) {
    if (now - last_time > expiration_) {
      WRENCH_WARN("Agent %s failed to send heartbeat (last at %.2f)", agent->getName().c_str(), last_time);
      job_scheduling_agent_->commport->dputMessage(new HeartbeatFailureNotificationMessage(agent));
    }
  }
}

int HeartbeatMonitorAgent::main()
{
  TerminalOutput::setThisProcessLoggingColor(TerminalOutput::COLOR_YELLOW);
  simgrid::s4u::this_actor::on_exit(
      [](bool /*failed*/) { XBT_DEBUG("My job scheduling agent has been killed! Have to die too!"); });

  WRENCH_INFO("Heartbeat Monitor Agent starting");

  this->setTimer(period_, "heartbeat_timer");

  while (true) {
    auto event = this->waitForNextEvent();

    if (auto timer_event = std::dynamic_pointer_cast<TimerEvent>(event)) {
      this->send_heartbeats();
      this->check_expired_heartbeats();
      this->setTimer(S4U_Simulation::getClock() + period_, "heartbeat_timer");
    }

    if (auto hb_event = std::dynamic_pointer_cast<CustomEvent>(event)) {
      if (auto hb_msg = std::dynamic_pointer_cast<HeartbeatMessage>(hb_event->message)) {
        last_heartbeat_time_[hb_msg->get_sender()] = S4U_Simulation::getClock();
        WRENCH_DEBUG("Received heartbeat from %s", hb_msg->get_sender()->getName().c_str());
      }
    }
  }

  return 0;
}

} // namespace wrench
