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
  // Initialize the simulation.
  auto simulation = wrench::Simulation::createSimulation();
  simulation->init(&argc, argv);
  // Override WRENCH log formatting
  xbt_log_control_set("root.fmt=[%12.6r]%e[%43a]%e[%26h]%e%e%m%n");

  // Parsing of the command-line arguments
  if (argc < 4) {
    std::cerr << "Usage: " << argv[0]
              << " <json job description list> <xml platform file> <scheduling policy name> [<python script name>]"
                 "[--log=workload_submission_agent.t:info]"
                 "[--log=job_lifecycle_tracker_agent.t:info]"
                 "[--log=job_scheduling_agent.t::info]"

              << std::endl;
    exit(1);
  }

  // The first command-line argument is the name of json file produced by the workload generator
  std::string job_list = argv[1];

  // Reading and parsing the platform description file, written in XML following the SimGrid-defined DTD,
  // to instantiate the simulated platform
  simulation->instantiatePlatform(argv[2]);

  // Instantiate a job lifecycle tracker that will be notified at the different stages of a job lifecycle
  auto job_lifecycle_tracker_agent = simulation->add(new wrench::JobLifecycleTrackerAgent("ASCR.doe.gov", job_list));

  // Retrieve the different hpc_systems from the platform description and create batch services and scheduling agents
  auto hpc_systems = wrench::Simulation::getHostnameListByCluster();
  WRENCH_INFO("%lu HPC systems in the platform", hpc_systems.size());

  // Create the networks of job scheduling and hearbeat monitor agents
  std::vector<std::shared_ptr<wrench::JobSchedulingAgent>> job_scheduling_agent_network;
  std::vector<std::shared_ptr<wrench::HeartbeatMonitorAgent>> heartbeat_monitor_agent_network;
  for (const auto& c : hpc_systems) {
    // Create a Scheduling Policy for this simulation run
    std::shared_ptr<SchedulingPolicy> scheduling_policy;
    if (argc == 5)
      scheduling_policy = SchedulingPolicy::create_scheduling_policy(argv[3], argv[4]);
    else
      scheduling_policy = SchedulingPolicy::create_scheduling_policy(argv[3]);

    // Create the HPCSystemDescription
    auto system_name = std::get<0>(c);
    auto system_type = HPCSystemDescription::string_to_hpc_system_type(
        wrench::S4U_Simulation::getClusterProperty(std::get<0>(c), "type"));
    auto system_num_compute_nodes = std::get<1>(c).size() - 1;
    auto system_memory_amount_in_gb =
        std::stoi(wrench::S4U_Simulation::getClusterProperty(std::get<0>(c), "memory_amount_in_gb"));
    auto system_storage_amount_in_gb =
        std::stod(wrench::S4U_Simulation::getClusterProperty(std::get<0>(c), "storage_amount_in_gb"));
    auto system_has_gpu = (wrench::S4U_Simulation::getClusterProperty(std::get<0>(c), "has_gpu") == "True");
    auto system_network_interconnect =
        wrench::S4U_Simulation::getClusterProperty(std::get<0>(c), "network_interconnect");

    auto system_description = std::make_shared<HPCSystemDescription>(
        system_name, system_type, system_num_compute_nodes, system_memory_amount_in_gb, system_storage_amount_in_gb,
        system_has_gpu, system_network_interconnect);

    // Instantiate a batch compute service on the computes node of this HPC system
    WRENCH_INFO("Creating BatchComputeService (with %5lu nodes) and JobSchedulingAgent on '%s'",
                system_num_compute_nodes, system_name.c_str());
    auto head_node = std::get<1>(c).front();
    std::vector<std::string> compute_nodes(std::get<1>(c).begin() + 1, std::get<1>(c).end());

    auto batch_service = simulation->add(new wrench::BatchComputeService(
        head_node, compute_nodes, "",
        {{wrench::BatchComputeServiceProperty::BATCH_SCHEDULING_ALGORITHM, "conservative_bf"}, {}}));

    // Instantiate a job scheduling agent on the head node of this HPC system
    auto new_agent = simulation->add(
        new wrench::JobSchedulingAgent(head_node, system_description, scheduling_policy, batch_service));
    new_agent->setDaemonized(true);

    // Allow this agent to notify the job lifecycle tracker
    new_agent->set_job_lifecycle_tracker(job_lifecycle_tracker_agent);

    // Add the new agent to the network
    job_scheduling_agent_network.push_back(new_agent);

    // Instantiate a heartbeat monitor agent on the head node of this HPC system
    auto new_hb_agent = simulation->add(new wrench::HeartbeatMonitorAgent(head_node, new_agent, 5., 15.));
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
      simulation->add(new wrench::WorkloadSubmissionAgent("ASCR.doe.gov", job_list, job_scheduling_agent_network));
  // Allow this agent to notify the job lifecycle tracker too
  workload_submission_agent->set_job_lifecycle_tracker(job_lifecycle_tracker_agent);

  // Launch the simulation. This call only returns when the simulation is complete
  try {
    simulation->launch();
  } catch (std::runtime_error& e) {
    std::cerr << "Exception: " << e.what() << std::endl;
    return 1;
  }

  return 0;
}
