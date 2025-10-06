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


  // Instantiate two batch compute service, and add them to the simulation.
  // TODO retrieve the number of clusters in the platform description
  auto clusters = wrench::S4U_Simulation::getAllClusterIDsByZone();
  exit(1);
  // TODO instantiate a batch compute service per cluster
  auto batch_service_1 = simulation->add(
      new wrench::BatchComputeService("Batch1HeadNode", {"Batch1ComputeNode1", "Batch1ComputeNode2"}, "", {}, {}));
  auto batch_service_2 = simulation->add(
      new wrench::BatchComputeService("Batch2HeadNode", {"Batch2ComputeNode1", "Batch2ComputeNode2"}, "", {}, {}));

  /* Instantiate an execution controller for each batch compute service */
  auto batch_controller_1 = simulation->add(new wrench::BatchServiceController("Batch1HeadNode", batch_service_1));
  auto batch_controller_2 = simulation->add(new wrench::BatchServiceController("Batch2HeadNode", batch_service_2));
  batch_controller_1->setPeer(batch_controller_2);
  batch_controller_1->setDaemonized(true);
  batch_controller_2->setPeer(batch_controller_1);
  batch_controller_2->setDaemonized(true);

  /* Instantiate an execution controller that will generate jobs */
  auto workload_submission_agent = simulation->add(
      new wrench::WorkloadSubmissionAgent("UserHost", job_list, {batch_controller_1, batch_controller_2}));
  batch_controller_1->setJobOriginator(workload_submission_agent);
  batch_controller_2->setJobOriginator(workload_submission_agent);

  /* Launch the simulation. This call only returns when the simulation is complete. */
  try {
    simulation->launch();
  } catch (std::runtime_error& e) {
    std::cerr << "Exception: " << e.what() << std::endl;
    return 1;
  }

  return 0;
}
