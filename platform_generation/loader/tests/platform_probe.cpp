#include <simgrid/s4u.hpp>
#include <fsmod/FileSystem.hpp>
#include <nlohmann/json.hpp>
#include <xbt/parse_units.hpp>
#include <xbt/config.hpp>
#include <execinfo.h>
#include <signal.h>
#include <unistd.h>
#include <algorithm>
#include <cmath>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <stdexcept>

namespace sg = simgrid::s4u;
namespace fs = simgrid::fsmod;
using json = nlohmann::json;

void require(bool value, const std::string& message) {
  if (!value) throw std::runtime_error(message);
}
bool near(double a, double b) { return std::abs(a-b) <= std::max(1e-12, std::abs(b)*1e-10); }
double bw(const std::string& text) { return xbt_parse_get_bandwidth("probe",0,text,""); }
double latency(const std::string& text) { return xbt_parse_get_time("probe",0,text,""); }
double speed(const std::string& text) { return xbt_parse_get_speed("probe",0,text,""); }

int main(int argc, char** argv) {
  signal(SIGSEGV, [](int) { void* trace[40]; int count=backtrace(trace,40); backtrace_symbols_fd(trace,count,2); _exit(139); });
  try {
    const std::string library=argv[1], mode=argv[2];
    const std::string first=argc>3?argv[3]:"", second=argc>4?argv[4]:"";
    json config; std::ifstream(std::getenv("PLATFORM_CONFIG")) >> config;
    sg::Engine engine(&argc,argv);
    engine.load_platform(library);
    engine.seal_platform();
    signal(SIGSEGV, [](int) { void* trace[40]; int count=backtrace(trace,40); backtrace_symbols_fd(trace,count,2); _exit(139); });
    std::map<std::string,std::shared_ptr<fs::FileSystem>> filesystems;
    std::map<std::string,sg::NetZone*> zones;
    for (auto* zone:engine.get_all_netzones()) {
      zones[zone->get_name()]=zone;
      for (const auto& [name,value]:fs::FileSystem::get_file_systems_by_netzone(zone)) filesystems[name]=value;
    }
    if (mode=="audit") {
      json report={{"hosts",json::array()},{"disks",json::array()},{"links",json::array()},
                   {"filesystems",json::array()},{"zone_properties",json::object()}};
      std::set<std::string> expected_hosts,expected_disks,expected_links;
      auto check_link=[&](const std::string& name,const std::string& bandwidth,const std::string& delay,bool shared) {
        auto* link=sg::Link::by_name(name);
        require(expected_links.insert(name).second,"Duplicate expected link: "+name);
        require(near(link->get_bandwidth(),bw(bandwidth)),"Link bandwidth: "+name);
        require(near(link->get_latency(),latency(delay)),"Link latency: "+name);
        require(link->get_sharing_policy()==(shared?sg::Link::SharingPolicy::SHARED:sg::Link::SharingPolicy::FATPIPE),"Link sharing policy: "+name);
        report["links"].push_back({{"name",name},{"bandwidth_bytes_per_second",link->get_bandwidth()},
          {"latency_seconds",link->get_latency()},{"sharing_policy",shared?"SHARED":"FATPIPE"}});
      };
      for (const auto& facility:config["facilities"]) {
        for (const auto& cluster:facility["clusters"]) {
          const std::string name=cluster["name"];
          report["zone_properties"][name]=*zones.at(name)->get_properties();
          if (cluster.contains("properties"))
            for (const auto& [key,value]:cluster.at("properties").items())
              require(zones.at(name)->get_property(key) && zones.at(name)->get_property(key)==value.get<std::string>(),
                      "Cluster property: "+name+"/"+key);
          check_link(name+"_backbone",cluster["backbone"]["bandwidth"],cluster["backbone"].value("latency","0s"),true);
          for (int i=0;i<cluster["count"].get<int>();++i) {
            const std::string hostname=cluster["prefix"].get<std::string>()+std::to_string(i)+cluster["suffix"].get<std::string>();
            auto* host=sg::Host::by_name(hostname); expected_hosts.insert(hostname);
            require(host->get_core_count()==cluster["node"]["cores"].get<int>(),"Host cores: "+hostname);
            require(near(host->get_speed(),speed(cluster["node"]["speed"])),"Host speed: "+hostname);
            if (cluster["node"].contains("memory_gb"))
              require(host->get_property("mem") && host->get_property("mem")==
                      std::to_string(cluster["node"]["memory_gb"].get<int>())+"GB", "Host RAM: "+hostname);
            report["hosts"].push_back({{"name",hostname},{"cores",host->get_core_count()},{"speed",host->get_speed()},{"properties",*host->get_properties()}});
            for (auto suffix:{"_LinkUP","_LinkDOWN"})
              check_link(hostname+suffix,cluster["node"]["private_link"]["bandwidth"],cluster["node"]["private_link"].value("latency","0s"),true);
            check_link(hostname+"_loopback",cluster["node"]["loopback"]["bandwidth"],cluster["node"]["loopback"].value("latency","0s"),false);
          }
        }
        for (const auto& storage:facility["storage_systems"]) {
          const std::string name=storage["name"], hostname=name+"_server";
          auto* host=sg::Host::by_name(hostname); expected_hosts.insert(hostname);
          require(near(host->get_speed(),speed(storage["server_speed"])),"Storage server speed: "+hostname);
          if (storage.contains("server_memory_gb"))
            require(host->get_property("mem") && host->get_property("mem")==
                    std::to_string(storage["server_memory_gb"].get<int>())+"GB", "Storage server RAM: "+hostname);
          report["hosts"].push_back({{"name",hostname},{"cores",host->get_core_count()},{"speed",host->get_speed()},
                                    {"properties",*host->get_properties()}});
          const int count=storage["disk_count"];
          require(host->get_disks().size()==static_cast<size_t>(count),"Disk count: "+name);
          for (auto* disk:host->get_disks()) {
            expected_disks.insert(disk->get_name());
            require(near(disk->get_read_bandwidth(),bw(storage["read_bandwidth"])),"Disk read bandwidth: "+disk->get_name());
            require(near(disk->get_write_bandwidth(),bw(storage["write_bandwidth"])),"Disk write bandwidth: "+disk->get_name());
            report["disks"].push_back({{"name",disk->get_name()},{"host",hostname},
              {"read_bytes_per_second",disk->get_read_bandwidth()},{"write_bytes_per_second",disk->get_write_bandwidth()}});
          }
        }
        for (const auto& link:facility["links"]) check_link(link["name"],link["bandwidth"],link.value("latency","0s"),true);
      }
      for (const auto& link:config["links"]) check_link(link["name"],link["bandwidth"],link.value("latency","0s"),true);
      require(engine.get_all_hosts().size()==expected_hosts.size(),"Total host count");
      require(engine.get_all_links().size()==expected_links.size()+1,"Total link count including SimGrid loopback");
      require(filesystems.size()==config["filesystems"].size(),"Total filesystem count");
      for (const auto& entry:config["filesystems"]) {
        auto filesystem=filesystems.at(entry["name"]);
        auto partition=filesystem->get_partition_for_path_or_null(entry["mount_point"]);
        const auto capacity=static_cast<sg_size_t>(xbt_parse_get_size("probe",0,entry["size"],""));
        require(partition && partition->get_size()==capacity,"Filesystem capacity: "+entry["name"].get<std::string>());
        require(partition->get_free_space()==capacity,"Initial free space");
        std::string path=entry["mount_point"].get<std::string>()+"probe-metadata";
        filesystem->create_file(path,sg_size_t(4096));
        require(partition->get_free_space()==capacity-4096,"Create occupancy");
        // FSMOD truncate_file removes the requested number of bytes.
        filesystem->truncate_file(path,sg_size_t(1024));
        require(partition->get_free_space()==capacity-3072,"Truncate restores removed bytes");
        filesystem->unlink_file(path);
        require(partition->get_free_space()==capacity,"Delete restores capacity");
        bool rejected=false;
        try { filesystem->create_file(path,capacity+1); } catch(const std::exception&) { rejected=true; }
        require(rejected && partition->get_free_space()==capacity,"Reject over-capacity allocation");
        report["filesystems"].push_back({{"name",entry["name"]},{"mount_point",entry["mount_point"]},
          {"capacity_bytes",capacity},{"free_bytes",partition->get_free_space()},{"occupancy_checks","passed"}});
      }
      report["status"]="passed"; std::cout << report.dump(2) << std::endl;
    } else if (mode=="audit-routes") {
      struct Endpoint { std::string facility, zone; bool cluster; };
      std::map<std::string,Endpoint> endpoints;
      using Pair=std::pair<std::string,std::string>;
      std::map<Pair,std::vector<std::string>> local, wan;
      std::map<std::string,std::vector<std::string>> gateway;
      for (const auto& facility:config["facilities"]) {
        const std::string name=facility["name"];
        for (const auto& zone:facility["clusters"])
          for (int i=0;i<zone["count"].get<int>();++i)
            endpoints[zone["prefix"].get<std::string>()+std::to_string(i)+zone["suffix"].get<std::string>()]={name,zone["name"],true};
        for (const auto& zone:facility["storage_systems"])
          endpoints[zone["name"].get<std::string>()+"_server"]={name,zone["name"],false};
        for (const auto& route:facility["routes"]) {
          auto path=route["links"].get<std::vector<std::string>>();
          local[{route["src"],route["dst"]}]=path;
          std::reverse(path.begin(),path.end());local[{route["dst"],route["src"]}]=path;
        }
        for (const auto& route:facility["wan_gateway"]["routes"])
          gateway[route["zone"]]=route["links"].get<std::vector<std::string>>();
      }
      for (const auto& route:config["routes"]) {
        auto path=route["links"].get<std::vector<std::string>>();
        wan[{route["src"],route["dst"]}]=path;
        std::reverse(path.begin(),path.end());wan[{route["dst"],route["src"]}]=path;
      }
      int checked=0,cross=0;
      for (const auto& [source,a]:endpoints) for (const auto& [destination,b]:endpoints) {
        if (source==destination) continue;
        std::vector<std::string> expected;
        auto append=[&](const auto& path) { expected.insert(expected.end(),path.begin(),path.end()); };
        if (a.cluster) { expected.push_back(source+"_LinkUP");expected.push_back(a.zone+"_backbone"); }
        if (a.facility!=b.facility) {
          append(gateway.at(a.zone));append(wan.at({a.facility,b.facility}));
          auto path=gateway.at(b.zone);std::reverse(path.begin(),path.end());append(path);++cross;
        } else if (a.zone!=b.zone) append(local.at({a.zone,b.zone}));
        if (b.cluster) {
          if (a.zone!=b.zone) expected.push_back(b.zone+"_backbone");
          expected.push_back(destination+"_LinkDOWN");
        }
        auto [actual,delay]=sg::Host::by_name(source)->route_to(sg::Host::by_name(destination));
        require(actual.size()==expected.size(),"Route length: "+source+" -> "+destination);
        double expected_delay=0;
        for (size_t i=0;i<expected.size();++i) {
          auto* link=sg::Link::by_name(expected[i]);
          require(actual[i]==link,"Route link identity/order: "+source+" -> "+destination+" at "+expected[i]);
          expected_delay+=link->get_latency();
        }
        require(near(delay,expected_delay),"Route latency: "+source+" -> "+destination);++checked;
      }
      std::cout << json({{"status","passed"},{"ordered_routes_checked",checked},{"cross_facility_routes",cross}}).dump() << std::endl;
    } else if (mode=="route") {
      auto [links,delay]=sg::Host::by_name(first)->route_to(sg::Host::by_name(second));
      std::cout << json({{"links",links.size()},{"latency_seconds",delay}}).dump() << std::endl;
    } else if (mode=="contention" || mode=="cross-pair-contention") {
      require(argc>=8,"Contention probe requires two endpoint pairs and a bottleneck link");
      auto* source=sg::Host::by_name(first); auto* destination=sg::Host::by_name(second);
      auto* peer_source=sg::Host::by_name(argv[5]); auto* peer_destination=sg::Host::by_name(argv[6]);
      auto* bottleneck=sg::Link::by_name(argv[7]);
      require(std::set<sg::Host*>{source,destination,peer_source,peer_destination}.size()==4,"Contention endpoints must be distinct");
      auto facility=[](const sg::Host* host) {
        const auto* parent=host->get_englobing_zone()->get_parent();
        require(parent!=nullptr,"Contention endpoint must belong to a facility child zone");
        return parent->get_name();
      };
      const auto source_facility=facility(source), destination_facility=facility(destination);
      const auto peer_source_facility=facility(peer_source), peer_destination_facility=facility(peer_destination);
      const std::set<std::string> pair{source_facility,destination_facility};
      const std::set<std::string> peer_pair{peer_source_facility,peer_destination_facility};
      const bool different_facility_pairs=pair!=peer_pair;
      if (mode=="cross-pair-contention")
        require(pair.size()==2 && peer_pair.size()==2 && different_facility_pairs,
                "Cross-pair contention requires two different inter-facility pairs");
      require(bottleneck->get_sharing_policy()==sg::Link::SharingPolicy::SHARED,"WAN bottleneck must be shared");
      auto [path,delay]=source->route_to(destination);
      auto [peer_path,peer_delay]=peer_source->route_to(peer_destination);
      require(std::count(path.begin(),path.end(),bottleneck)==1 &&
              std::count(peer_path.begin(),peer_path.end(),bottleneck)==1,"Both routes must use the same physical link object");
      // The two private endpoint links are independent. Every other common
      // link must be at least as fast as the selected physical WAN bottleneck.
      for (auto* link:path)
        if (std::find(peer_path.begin(),peer_path.end(),link)!=peer_path.end())
          require(link->get_bandwidth()>=bottleneck->get_bandwidth(),"Unexpected slower common link");
      source->add_actor("wan-contention-probe",[=] {
        // Simulated payload size only: no real payload buffer is allocated.
        constexpr uint64_t bytes=uint64_t{4}*1024*1024*1024;
        double start=sg::Engine::get_clock();
        sg::Comm::sendto(source,destination,bytes);
        double baseline=sg::Engine::get_clock()-start;
        start=sg::Engine::get_clock();
        sg::Comm::sendto(peer_source,peer_destination,bytes);
        double peer_baseline=sg::Engine::get_clock()-start;
        auto barrier=sg::Barrier::create(3);
        double duration=0,peer_duration=0,flow_start=0,peer_start=0;
        auto transfer=source->add_actor("first-contention-flow",[=,&duration,&flow_start] {
          barrier->wait();flow_start=sg::Engine::get_clock();
          sg::Comm::sendto(source,destination,bytes);
          duration=sg::Engine::get_clock()-flow_start;
        });
        auto peer_transfer=peer_source->add_actor("peer-contention-flow",[=,&peer_duration,&peer_start] {
          barrier->wait();peer_start=sg::Engine::get_clock();
          sg::Comm::sendto(peer_source,peer_destination,bytes);
          peer_duration=sg::Engine::get_clock()-peer_start;
        });
        start=sg::Engine::get_clock();
        barrier->wait();transfer->join();peer_transfer->join();
        double concurrent=sg::Engine::get_clock()-start;
        double aggregate=2.0*bytes/concurrent;
        require(near(flow_start,peer_start),"Contention flows must start simultaneously");
        require(duration>baseline*1.05,"First flow did not slow down relative to its isolated baseline");
        require(peer_duration>peer_baseline*1.05,"Peer flow did not slow down relative to its isolated baseline");
        require(aggregate<=bottleneck->get_bandwidth()*(1.0+1e-6),"Aggregate transfer rate exceeds shared WAN capacity");
        std::vector<std::string> path_names,peer_path_names;
        for (const auto* link:path) path_names.push_back(link->get_name());
        for (const auto* link:peer_path) peer_path_names.push_back(link->get_name());
        std::cout << json({{"status","passed"},{"physical_link",bottleneck->get_name()},
          {"bytes_per_flow",bytes},{"single_flow_seconds",baseline},{"two_flows_seconds",concurrent},
          {"peer_single_flow_seconds",peer_baseline},{"first_concurrent_seconds",duration},
          {"peer_concurrent_seconds",peer_duration},{"different_facility_pairs",different_facility_pairs},
          {"source_facility",source_facility},{"destination_facility",destination_facility},
          {"peer_source_facility",peer_source_facility},{"peer_destination_facility",peer_destination_facility},
          {"source_path",path_names},{"peer_path",peer_path_names},
          {"same_physical_link_object",true},{"simultaneous_start",true},
          {"aggregate_gbps_including_latency",aggregate*8/1e9},
          {"transport_tcp_window_bytes",simgrid::config::get_value<double>("network/TCP-gamma")},
          {"physical_capacity_gbps",bottleneck->get_bandwidth()*8/1e9}}).dump() << std::endl;
      });
      engine.run();
    } else if (mode=="comm") {
      auto* source=sg::Host::by_name(first); auto* destination=sg::Host::by_name(second);
      source->add_actor("communication-probe",[=] {
        constexpr uint64_t size=2*1024*1024;
        double start=sg::Engine::get_clock();
        sg::Comm::sendto(source,destination,size);
        sg::Comm::sendto(destination,source,size);
        std::cout << json({{"status","passed"},{"source",first},{"destination",second},{"bytes_each_way",size},
          {"simulated_seconds",sg::Engine::get_clock()-start}}).dump() << std::endl;
      });
      engine.run();
    } else if (mode=="filesystem-clients") {
      const std::string reader_host=argv[5];
      auto filesystem=filesystems.at(first);
      std::string mount;
      for (const auto& entry:config["filesystems"]) if (entry["name"]==first) mount=entry["mount_point"];
      const auto partition=filesystem->get_partition_for_path_or_null(mount);
      const auto free=partition->get_free_space();
      const std::string path=mount+"probe-shared-client";
      constexpr sg_size_t bytes=1024*1024;
      sg::Host::by_name(second)->add_actor("file-writer",[=] {
        auto file=filesystem->open(path,"w"); require(file->write(bytes)==bytes,"Shared writer byte count"); file->close();
        sg::Mailbox::by_name("file-ready")->put(new int(1),1);
      });
      sg::Host::by_name(reader_host)->add_actor("file-reader",[=] {
        delete sg::Mailbox::by_name("file-ready")->get<int>();
        require(filesystem->file_exists(path) && filesystem->file_size(path)==bytes,"Writer data visible to reader");
        require(partition->get_free_space()==free-bytes,"Shared occupancy");
        auto file=filesystem->open(path,"r"); require(file->read(bytes)==bytes,"Shared reader byte count"); file->close();
        filesystem->unlink_file(path); require(partition->get_free_space()==free,"Shared delete restores free space");
        std::cout << json({{"status","passed"},{"filesystem",first},{"writer",second},{"reader",reader_host},
          {"bytes_written_and_read",bytes}}).dump() << std::endl;
      });
      engine.run();
    } else if (mode=="filesystem") {
      auto filesystem=filesystems.at(first);
      std::string mount;
      for (const auto& entry:config["filesystems"]) if (entry["name"]==first) mount=entry["mount_point"];
      sg::Host::by_name(second)->add_actor("filesystem-probe",[=] {
        auto visible=fs::FileSystem::get_file_systems_by_actor(sg::ActorPtr(sg::Actor::self()));
        std::cout << json({{"filesystem",first},{"actor_host",second},{"visible_via_actor_lookup",visible.count(first)>0}}).dump() << std::endl;
        constexpr sg_size_t bytes=1024*1024;
        auto partition=filesystem->get_partition_for_path_or_null(mount);
        auto free=partition->get_free_space(); std::string path=mount+"probe-io";
        double start=sg::Engine::get_clock();
        auto file=filesystem->open(path,"w");
        require(file->write(bytes)==bytes,"Write byte count"); file->close();
        require(filesystem->file_size(path)==bytes && partition->get_free_space()==free-bytes,"Write occupancy");
        file=filesystem->open(path,"r"); require(file->read(bytes)==bytes,"Read byte count"); file->close();
        filesystem->unlink_file(path); require(partition->get_free_space()==free,"Delete after IO");
        std::cout << json({{"status","passed"},{"filesystem",first},{"actor_host",second},{"bytes_written_and_read",bytes},
          {"simulated_seconds",sg::Engine::get_clock()-start}}).dump() << std::endl;
      });
      engine.run();
    } else throw std::runtime_error("Unknown probe mode");
    return 0;
  } catch(const std::exception& error) { std::cerr << "PROBE ERROR: " << error.what() << std::endl; return 1; }
}
