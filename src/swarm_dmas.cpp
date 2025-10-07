#include <iostream>
#include <wrench.h>

#include "JobSchedulingAgent.h"
#include "WorkloadSubmissionAgent.h"

WRENCH_LOG_CATEGORY(swarm_dmas, "Log category for SWARM Distributed Multi-Agent Scheduling simulator");

int main(int argc, char** argv)
{
  //Initialize the simulation.
  auto simulation = wrench::Simulation::createSimulation();
  simulation->init(&argc, argv);
  // Override WRENCH log formatting
  xbt_log_control_set("root.fmt=[%12.6r]%e[%41a]%e[%26h]%e%e%m%n");

  // Parsing of the command-line arguments
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0]
              << " <json job description list> <xml platform file> "
                 "[--log=job_scheduling_agent.t=info --log=workload_submission_agent.t=info]"
              << std::endl;
    exit(1);
  }

  // The first command-line argument is the name of json file produced by the workload generator 
  std::string job_list = argv[1];

  // Reading and parsing the platform description file, written in XML following the SimGrid-defined DTD,
  // to instantiate the simulated platform
  simulation->instantiatePlatform(argv[2]);

  // Retrieve the different clusters from the platform description and create batch services and scheduling agents
  auto clusters = wrench::Simulation::getHostnameListByCluster();

  std::vector<std::shared_ptr<wrench::JobSchedulingAgent>> job_scheduling_agents;
    
  WRENCH_INFO("%lu HPC systems in the platform", clusters.size());

  for (const auto& c : clusters) {
    std::string head_node = std::get<1>(c).front();
    std::vector<std::string> compute_nodes(std::get<1>(c).begin() + 1, std::get<1>(c).end());
    WRENCH_INFO("Creating BatchComputeService (with %5lu nodes) and JobSchedulingAgent on '%s'", compute_nodes.size(), std::get<0>(c).c_str());
    // Instantiate a batch compute service on the computes node of this cluster
    auto batch_service = simulation->add(new wrench::BatchComputeService(head_node, compute_nodes, "", {}, {}));

    // Instantiate a scheduling agent on the head node of this cluster
    job_scheduling_agents.push_back(simulation->add(new wrench::JobSchedulingAgent(std::get<0>(c), head_node, batch_service)));
  }

  // Create the network of scheduling agents 
  for (const auto& src : job_scheduling_agents) {
    for (const auto& dst : job_scheduling_agents)
      if (src!=dst)
        src->add_peer(dst);
    src->setDaemonized(true);
  }
  
  // Instantiate an workload submission agent that will generate jobs and assign jobs to scheduling agents
  auto workload_submission_agent = simulation->add(
      new wrench::WorkloadSubmissionAgent("WSAgent", job_list, job_scheduling_agents));
  for (const auto& agent : job_scheduling_agents)
    agent->setJobOriginator(workload_submission_agent);
  
  // Launch the simulation. This call only returns when the simulation is complete
  try {
    simulation->launch();
  } catch (std::runtime_error& e) {
    std::cerr << "Exception: " << e.what() << std::endl;
    return 1;
  }

  return 0;
}
