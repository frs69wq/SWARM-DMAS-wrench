#include <fstream>
#include <iostream>
#include <simgrid/s4u/Host.hpp>
#include <wrench.h>

#include "agents/HeartbeatMonitorAgent.h"
#include "agents/JobLifecycleTrackerAgent.h"
#include "agents/JobSchedulingAgent.h"
#include "agents/WorkloadSubmissionAgent.h"
#include "info/HPCSystemDescription.h"
#include "policies/SchedulingPolicy.h"

WRENCH_LOG_CATEGORY(swarm_dmas, "Log category for SWARM Distributed Multi-Agent Scheduling simulator");

int main(int argc, char** argv)
{
  // Parse command-line arguments
  if (argc < 2) {
    std::cerr << "Usage: " << argv[0] << " <experiment_description.json>"
                 "[--log=workload_submission_agent.t:info]"
                 "[--log=job_lifecycle_tracker_agent.t:info]"
                 "[--log=job_scheduling_agent.t::info]"
              << std::endl;
    exit(1);
  }
  
  // Parse the experiment description JSON file
  std::ifstream file(argv[1]);
  if (!file.is_open()) {
    std::cerr << "Failed to open file: " << argv[1] << std::endl;
    exit(1);
  }

  nlohmann::json j;
  file >> j;

  std::string workload = j["workload"];
  std::string platform = j["platform"];
  std::string policy = j["policy"];
  double hearbeat_period = j["hearbeat_period"].get<double>();
  double heartbeat_expiration = j["heartbeat_expiration"].get<double>();
  std::string hardware_failure_profile = j["hardware_failure_profile"];

  // Optional field
  std::string bidder = j.value("bidder", ""); // Default to empty string if not present

  // Initialize the simulation.
  auto simulation = wrench::Simulation::createSimulation();
  simulation->init(&argc, argv);
  // Override WRENCH log formatting
  xbt_log_control_set("root.fmt=[%12.6r]%e[%43a]%e[%26h]%e%e%m%n");

  // Instantiate the simulated platform
  simulation->instantiatePlatform(platform);

  // Instantiate a job lifecycle tracker that will be notified at the different stages of a job lifecycle
  auto job_lifecycle_tracker_agent = simulation->add(new wrench::JobLifecycleTrackerAgent("ASCR.doe.gov", workload));

  // Retrieve the different HPC systems from the platform description
  // Create the networks of job scheduling and hearbeat monitor agents
  std::vector<std::shared_ptr<wrench::JobSchedulingAgent>> job_scheduling_agent_network;
  std::vector<std::shared_ptr<wrench::HeartbeatMonitorAgent>> heartbeat_monitor_agent_network;
  for (const auto& [system_name, host_list] : wrench::Simulation::getHostnameListByCluster()) {
    // Create the HPCSystemDescription
    auto system_description = HPCSystemDescription::create(system_name, host_list);

    // Instantiate a batch compute service on the computes node of this HPC system
    auto head_node = host_list.front();
    std::vector<std::string> compute_nodes(host_list.begin() + 1, host_list.end());
    auto batch_service = simulation->add(new wrench::BatchComputeService(
        head_node, compute_nodes, "",
        {{wrench::BatchComputeServiceProperty::BATCH_SCHEDULING_ALGORITHM, "conservative_bf"}, {}}));

    // Create a Scheduling Policy for this simulation run
    auto scheduling_policy = SchedulingPolicy::create_scheduling_policy(policy, bidder);
    
    // Instantiate a job scheduling agent on the head node of this HPC system
    auto new_agent = simulation->add(
        new wrench::JobSchedulingAgent(head_node, system_description, scheduling_policy, batch_service));
    new_agent->setDaemonized(true);
    // Allow this agent to notify the job lifecycle tracker
    new_agent->set_job_lifecycle_tracker(job_lifecycle_tracker_agent);
    // Add the new agent to the network
    job_scheduling_agent_network.push_back(new_agent);

    // Instantiate a heartbeat monitor agent on the head node of this HPC system
    auto new_hb_agent = 
      simulation->add(new wrench::HeartbeatMonitorAgent(head_node, new_agent, hearbeat_period, heartbeat_expiration));
    new_hb_agent->setDaemonized(true);
    // Attach the heartbeat monitor to the job scheduling agent
    new_agent->set_heartbeat_monitor(new_hb_agent);
    // Add the new agent to the network
    heartbeat_monitor_agent_network.push_back(new_hb_agent);
  }

  // Assign the network to the scheduling policy for each agent
  for (const auto& agent : job_scheduling_agent_network)
    agent->set_scheduling_policy_network(job_scheduling_agent_network);

  // Let the heartbeat monitor agents know each others
  for (const auto& src : heartbeat_monitor_agent_network)
    for (const auto& dst : heartbeat_monitor_agent_network)
      if (src != dst)
        src->add_heartbeat_monitor_agent(dst);

  // Instantiate an workload submission agent that will generate jobs and assign jobs to scheduling agents
  auto workload_submission_agent =
      simulation->add(new wrench::WorkloadSubmissionAgent("ASCR.doe.gov", workload, job_scheduling_agent_network));
  // Allow this agent to notify the job lifecycle tracker too
  workload_submission_agent->set_job_lifecycle_tracker(job_lifecycle_tracker_agent);

  // Launch the simulation. This call only returns when the simulation is complete
  try {
    simulation->launch();
    return 0;
  } catch (std::runtime_error& e) {
    std::cerr << "Exception: " << e.what() << std::endl;
    return 1;
  }
}
