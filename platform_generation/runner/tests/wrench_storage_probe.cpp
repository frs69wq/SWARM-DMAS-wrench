#include "../PlatformLoading.hpp"
#include <wrench/failure_causes/StorageServiceNotEnoughSpace.h>
#include <wrench/execution_controller/ExecutionController.h>
#include <wrench/exceptions/ExecutionException.h>
#include <nlohmann/json.hpp>
#include <fsmod/JBODStorage.hpp>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <map>
#include <numeric>
#include <set>

namespace sg = simgrid::s4u;
namespace fs = simgrid::fsmod;
using json = nlohmann::json;

void require(bool condition, const std::string& message) {
  if (!condition) throw std::runtime_error(message);
}

struct IoCheck {
  swarm_platform::StorageBinding binding;
  std::shared_ptr<wrench::SimpleStorageService> service;
  std::shared_ptr<wrench::FileLocation> location;
  sg::SemaphorePtr written = sg::Semaphore::create(0);
  std::map<std::string, unsigned> disk_reads, disk_writes;
  json report = json::object();
  std::string error;
  bool writer_finished = false, reader_finished = false;
  sg_size_t initial_free = 0;
  static constexpr sg_size_t bytes = 8 * 1024 * 1024;
};

class StorageClient : public wrench::ExecutionController {
  std::shared_ptr<IoCheck> check;
  bool writer;

public:
  StorageClient(const std::string& host, std::shared_ptr<IoCheck> state, bool writes)
      : ExecutionController(host, writes ? "storage-writer-check" : "storage-reader-check"),
        check(std::move(state)), writer(writes) {}

  int main() override {
    if (!writer) check->written->acquire();
    try {
      if (!check->error.empty()) return 0;
      const auto& binding = check->binding;
      auto partition = binding.filesystem->get_partition_for_path_or_null(binding.mount_point);
      if (writer) {
        require(check->service->getFileSystem() == binding.filesystem, "Storage service created a second filesystem");
        require(check->service->getTotalSpace() == partition->get_size(), "Service and FSMod capacity differ");
        require(check->service->getTotalFreeSpaceAtPath(binding.mount_point) == check->initial_free,
                "Initial service/FSMod occupancy differs");
        auto huge = wrench::Simulation::addFile("over-capacity-file", partition->get_size() + 1);
        bool rejected = false;
        try {
          wrench::StorageService::writeFileAtLocation(
              wrench::FileLocation::LOCATION(check->service, binding.mount_point, huge));
        } catch (wrench::ExecutionException& error) {
          rejected = std::dynamic_pointer_cast<wrench::StorageServiceNotEnoughSpace>(error.getCause()) != nullptr;
        }
        require(rejected && partition->get_free_space() == check->initial_free,
                "Over-capacity WRENCH write must fail without consuming space");
        check->report["over_capacity_write_rejected"] = true;
        auto start = sg::Engine::get_clock();
        wrench::StorageService::writeFileAtLocation(check->location);
        check->report["write_seconds"] = sg::Engine::get_clock() - start;
        require(partition->get_free_space() == check->initial_free - IoCheck::bytes, "Incorrect FSMod write occupancy");
        require(check->service->getTotalFreeSpaceAtPath(binding.mount_point) == partition->get_free_space(),
                "WRENCH/FSMod occupancy differs after write");
        require(binding.filesystem->file_size(check->location->getFilePath()) == IoCheck::bytes,
                "Wrong file size in the existing filesystem");
        check->writer_finished = true;
      } else {
        require(wrench::StorageService::lookupFileAtLocation(check->location), "Second client cannot find the file");
        auto start = sg::Engine::get_clock();
        wrench::StorageService::readFileAtLocation(check->location);
        check->report["read_seconds"] = sg::Engine::get_clock() - start;
        require(partition->get_free_space() == check->initial_free - IoCheck::bytes, "Read changed occupancy");
        wrench::StorageService::deleteFileAtLocation(check->location);
        require(!binding.filesystem->file_exists(check->location->getFilePath()), "Delete left the file in FSMod");
        require(partition->get_free_space() == check->initial_free &&
                check->service->getTotalFreeSpaceAtPath(binding.mount_point) == check->initial_free,
                "Delete did not restore shared free space");
        check->reader_finished = true;
      }
    } catch (const std::exception& error) {
      check->error = error.what();
    }
    if (writer) check->written->release();
    return 0;
  }
};

// Independent synthetic platforms exercise both the new validation and the
// ordinary WRENCH checks. They do not alter generated FABRIC definitions.
void validation_check(const std::shared_ptr<wrench::Simulation>& simulation, const std::string& mode) {
  const std::map<std::string, std::string> errors = {
      {"legacy-duplicate", "same mount point"}, {"legacy-prefix", "overlapping mount points"},
      {"legacy-bad-size", "invalid size"}, {"unregistered-store", "same mount point"},
      {"extra-ordinary-disks", "same mount point"}, {"null-binding", "requires filesystem"},
      {"duplicate-disk", "multiply registered"}, {"duplicate-store", "Duplicate FSMod"},
      {"zero-capacity", "partition/capacity"}, {"unregistered-filesystem", "not registered"},
      {"empty-store", "no backing disks"}, {"mixed-hosts", "one storage host"}};
  require(mode == "legacy-valid" || errors.count(mode), "Unknown validation test case");
  try {
    simulation->instantiatePlatform([&] {
      auto* root = sg::Engine::get_instance()->get_netzone_root()->add_netzone_full("validation-zone");
      auto* host = root->add_host("validation-host", "1Gf");
      auto* first = host->add_disk("first", "1GBps", "1GBps");
      if (mode.rfind("legacy-", 0) == 0) {
        if (mode == "legacy-bad-size") first->set_property("size", "invalid-capacity");
        else {
          auto* second = host->add_disk("second", "1GBps", "1GBps");
          if (mode == "legacy-valid" || mode == "legacy-prefix") {
            first->set_property("mount", "/data/a")->set_property("size", "1GB");
            second->set_property("mount", mode == "legacy-prefix" ? "/data/a/sub" : "/data/b")
                ->set_property("size", "1GB");
          }
        }
      } else if (mode == "null-binding") {
        simulation->registerExistingFileSystem(nullptr, nullptr, "/store/");
      } else {
        auto* second_host = mode == "mixed-hosts" ? root->add_host("other-host", "1Gf") : host;
        auto* second = second_host->add_disk("second", "1GBps", "1GBps");
        std::vector<sg::Disk*> disks = mode == "duplicate-disk" ? std::vector<sg::Disk*>{first, first}
                                                               : std::vector<sg::Disk*>{first, second};
        if (mode == "empty-store") disks.clear();
        auto storage = fs::JBODStorage::create("validation-storage", disks);
        auto filesystem = fs::FileSystem::create("validation-fs");
        filesystem->mount_partition("/store/", storage, mode == "zero-capacity" ? "0B" : "1GB");
        if (mode != "unregistered-filesystem") fs::FileSystem::register_file_system(root, filesystem);
        if (mode != "unregistered-store")
          simulation->registerExistingFileSystem(filesystem, storage, "/store/");
        if (mode == "duplicate-store") simulation->registerExistingFileSystem(filesystem, storage, "/store/");
        if (mode == "extra-ordinary-disks") {
          host->add_disk("ordinary-a", "1GBps", "1GBps");
          host->add_disk("ordinary-b", "1GBps", "1GBps");
        }
      }
      root->seal();
    });
    require(mode == "legacy-valid", "Invalid storage configuration was accepted: " + mode);
  } catch (const std::invalid_argument& error) {
    require(mode != "legacy-valid" && std::string(error.what()).find(errors.at(mode)) != std::string::npos,
            "Unexpected validation failure: " + std::string(error.what()));
  }
  std::cout << json({{"status", "passed"}, {"validation_case", mode}}).dump() << std::endl;
}

int main(int argc, char** argv) {
  try {
    require(argc >= 3, "Usage: wrench_storage_probe LOADER validate CASE | io FILESYSTEM WRITER READER");
    const std::string loader = argv[1], mode = argv[2];
    auto simulation = wrench::Simulation::createSimulation();
    int init_argc = 1;
    simulation->init(&init_argc, argv);
    if (mode == "validate") {
      require(argc == 4, "Validation case is required");
      validation_check(simulation, argv[3]);
      return 0;
    }
    require(mode == "io" && argc == 6, "Filesystem and two client hosts are required");
    swarm_extension::load_platform(simulation, loader);
    const auto bindings = swarm_extension::storage_bindings(loader);
    const auto found = std::find_if(bindings.begin(), bindings.end(), [&](const auto& item) {
      return item.filesystem->get_name() == argv[3];
    });
    require(found != bindings.end(), "Unknown filesystem ID");
    auto check = std::make_shared<IoCheck>();
    check->binding = *found;
    const auto server = found->storage->get_first_disk()->get_host()->get_name();
    require(argv[4] != server && argv[5] != server && std::string(argv[4]) != argv[5],
            "Use two distinct remote clients");
    check->service = simulation->add(wrench::SimpleStorageService::createFsmodStorageService(
        server, found->filesystem));
    bool rejected = false;
    try {
      std::unique_ptr<wrench::SimpleStorageService> invalid(
          wrench::SimpleStorageService::createFsmodStorageService(
              server, found->filesystem, {{wrench::SimpleStorageServiceProperty::BUFFER_SIZE, "0B"}}));
    } catch (const std::invalid_argument&) { rejected = true; }
    require(rejected, "Unbuffered existing-filesystem service must be rejected");
    auto partition = found->filesystem->get_partition_for_path_or_null(found->mount_point);
    check->initial_free = partition->get_free_space();
    check->location = wrench::FileLocation::LOCATION(check->service, found->mount_point,
        wrench::Simulation::addFile("wrench-fsmod-check-file", IoCheck::bytes));
    for (const auto* disk : found->storage->get_disks()) {
      check->disk_reads[disk->get_name()] = 0;
      check->disk_writes[disk->get_name()] = 0;
    }
    sg::Io::on_completion_cb([check](const sg::Io& io) {
      if (io.get_state() == sg::Activity::State::FINISHED && check->disk_reads.count(io.get_name()))
        ++(check->writer_finished ? check->disk_reads : check->disk_writes)[io.get_name()];
    });
    simulation->add(new StorageClient(argv[4], check, true));
    simulation->add(new StorageClient(argv[5], check, false));
    simulation->launch();
    require(check->error.empty(), "Storage client failed: " + check->error);
    require(check->writer_finished && check->reader_finished, "Storage clients did not complete");
    json disk_ios = json::object();
    const auto chunks = (IoCheck::bytes + 999999) / 1000000;
    for (const auto& [name, reads] : check->disk_reads) {
      require(reads == chunks && check->disk_writes.at(name) == chunks,
              "Backing disk did not complete all read/write chunks: " + name);
      disk_ios[name] = {{"reads", reads}, {"writes", check->disk_writes.at(name)}};
    }
    check->report.update({{"status", "passed"}, {"filesystem", argv[3]}, {"writer", argv[4]}, {"reader", argv[5]},
        {"storage_server", server}, {"capacity_bytes", partition->get_size()}, {"file_bytes", IoCheck::bytes},
        {"restored_free_bytes", partition->get_free_space()}, {"same_filesystem_object", true},
        {"buffer_size_bytes", 1000000}, {"physical_disk_io_completions", disk_ios}});
    std::cout << check->report.dump() << std::endl;
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << std::endl;
    return 1;
  }
}
