/**
 ** This simulator simulates the execution of two batch compute services, each used by their own controller.
 ** The controllers "talk to each other" to delegate some jobs to the other controller.  There is a third
 ** controller that submits job requests to either of the previous two controllers.
 **
 ** The compute platform comprises seven hosts:
 **   - UserHost: runs the execution controller that creates job requests and sends them to one of the batch controllers
 **   - Batch1HeadNode, Batch1ComputeNode1,and Batch1ComputeNode2: the first batch system, which runs on Batch1HeadNode,
 *along with a controller
 **   - Batch2HeadNode, Batch2ComputeNode1,and Batch2ComputeNode2: the second batch system, which runs on
 *Batch2HeadNode, along with a controller
 **
 ** Example invocation of the simulator for 10 jobs, with no logging:
 **    ./wrench-example-batch-bag-of-actions 10 ./two_batch_services.xml
 **
 ** Example invocation of the simulator for 10 jobs, with only execution controller logging:
 **    ./wrench-example-batch-bag-of-actions 10 ./two_batch_services.xml --log=batch_service_controller.threshold=info
 *--log=job_generation_controller.threshold=info
 **
 ** Example invocation of the simulator for 10 jobs, with full logging:
 **    ./wrench-example-batch-bag-of-actions 10 ./two_batch_services.xml --wrench-full-log
 **/

#include <iostream>
#include <wrench.h>

#include "BatchServiceController.h"
#include "WorkloadSubmissionAgent.h"

int main(int argc, char** argv)
{
  //Initialize the simulation.
  auto simulation = wrench::Simulation::createSimulation();
  simulation->init(&argc, argv);

  // Parsing of the command-line arguments
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0]
              << " <json job description list> <xml platform file> "
                 "[--log=batch_service_controller.threshold=info "
                 "--log=job_generation_controller.threshold=info]"
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

  // TODO Rename BatchServiceController to SchedulingAgent
  std::vector<std::shared_ptr<wrench::BatchServiceController>> scheduling_agents;
    
  std::cout << clusters.size() << " clusters in the platform"<< std::endl;
  for (const auto& c : clusters) {
    std::cout << "Creating BatchComputeService and SchedulingAgent on " << std::get<0>(c) << std::endl;

    std::string head_node = std::get<1>(c).front();
    std::vector<std::string> compute_nodes(std::get<1>(c).begin() + 1, std::get<1>(c).end());

    // Instantiate a batch compute service on the computes node of this cluster
    auto batch_service = simulation->add(new wrench::BatchComputeService(head_node, compute_nodes, "", {}, {}));

    // Instantiate a scheduling agent on the head node of this cluster
    scheduling_agents.push_back(simulation->add(new wrench::BatchServiceController(head_node, batch_service)));
  }

  // Create the network of scheduling agents 
  for (const auto& src : scheduling_agents) {
    for (const auto& dst : scheduling_agents)
      if (src!=dst)
        src->setPeer(dst);
    src->setDaemonized(true);
  }
  
  // Instantiate an workload submission agent that will generate jobs and assign jobs to scheduling agents
  auto workload_submission_agent = simulation->add(
      new wrench::WorkloadSubmissionAgent("WSAgent", job_list, scheduling_agents));
  for (const auto& agent : scheduling_agents)
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
