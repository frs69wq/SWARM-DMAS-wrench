# SWARM-DMAS-wrench

**SWARM-DMAS-wrench** is a simulation framework built on top of WRENCH for evaluating decentralized meta-scheduling algorithms in distributed computing environments.

---

## 📁 Codebase Organization

```text
SWARM-DMAS-wrench/ 
 ├── src/
 │ ├── agents/ # Custom WRENCH agents (e.g., meta-schedulers, local schedulers)
 │ ├── policies/ # Scheduling policies (e.g., PureLocal, Cooperative) 
 │ └── swar_dmas.cpp # Entry point for simulation
 ├── data_generation/
 │ ├── data/ # Workload traces and input datasets 
 │ └── scripts/ # Scripts to generate synthetic workloads 
 ├── platforms/ 
 │ └── AmSC.xml # Platform description (resources, links, latencies) 
 ├── CMakeLists.txt # Build configuration
 └── README.md # Project documentation
```

 ---

## 🛠️ Installation

### Prerequisites

- C++17 compiler (e.g., `g++`, `clang++`)
- WRENCH installed and configured
- CMake ≥ 3.10
- Boost libraries

### Build Instructions

```bash
git clone https://github.com/frs69wq/SWARM-DMAS-wrench.git
cd SWARM-DMAS-wrench
mkdir build && cd build
cmake ..
make -j
cd ..
```

## 🚀 Running the Simulation

To run the simulation using the PureLocal policy on the smallest workload:

```bash
cd build
./swarm_dmas workloads/heterogeneous_mix_10.json platforms/AmSC.xml PureLocal
```

## 🧠 Agent Roles (in src/agents/)
Each agent extends WRENCH's simulation API to implement custom behaviors.

TBD


## 📊 Policy Roles (in src/policies/)
Policies define the decision-making logic used by the meta-scheduler or local schedulers.

 - PureLocal: Jobs are scheduled only on the system where they were submitted (baseline).
 - RandomBidding: Jobs are randomly assigned to another system.
