cmake_minimum_required(VERSION 3.16)
project(swarm_json_platform_loader LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/../../conf/cmake")
find_package(SimGrid 4.1 REQUIRED)
find_package(FSMod REQUIRED)
find_package(nlohmann_json REQUIRED)

add_library(swarm_platform SHARED json_platform_loader.cpp)
target_include_directories(swarm_platform PRIVATE ${SimGrid_INCLUDE_DIR} ${FSMOD_INCLUDE_DIR})
target_link_libraries(swarm_platform PRIVATE SimGrid::SimGrid FSMOD::FSMOD nlohmann_json::nlohmann_json ${CMAKE_DL_LIBS})
# Engine cleanup still calls code owned by the loader and FSMod. Retain their
# mappings until process exit even if the engine closes its plugin handle first.
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  target_link_options(swarm_platform PRIVATE "LINKER:-z,nodelete")
endif()

include(CTest)
if(BUILD_TESTING)
  find_package(Python3 COMPONENTS Interpreter REQUIRED)
  add_executable(platform_probe tests/platform_probe.cpp)
  target_include_directories(platform_probe PRIVATE ${SimGrid_INCLUDE_DIR} ${FSMOD_INCLUDE_DIR})
  target_link_libraries(platform_probe PRIVATE SimGrid::SimGrid FSMOD::FSMOD nlohmann_json::nlohmann_json)
  set(SWARM_TEST_PLATFORM "${CMAKE_CURRENT_SOURCE_DIR}/../generated/two_facilities/fabric_platform.json"
      CACHE FILEPATH "Generated ATLA/DALL platform for gateway integration checks")
  set(SWARM_MULTIHOP_TEST_PLATFORM "${CMAKE_CURRENT_SOURCE_DIR}/../generated/multihop_facilities/fabric_platform.json"
      CACHE FILEPATH "Generated ATLA/CLEM platform for multi-hop gateway checks")
  set(SWARM_FULL_TEST_PLATFORM "${CMAKE_CURRENT_SOURCE_DIR}/../generated/fabric_platform.json"
      CACHE FILEPATH "Generated full 27-facility platform for integration checks")
  function(gateway_check name platform)
    add_test(NAME ${name} COMMAND platform_probe $<TARGET_FILE:swarm_platform> ${ARGN})
    set_tests_properties(${name} PROPERTIES ENVIRONMENT "PLATFORM_CONFIG=${platform}" TIMEOUT 30)
  endfunction()
  function(gateway_suite prefix platform remote)
    gateway_check(${prefix}resource_audit "${platform}" audit)
    gateway_check(${prefix}route_audit "${platform}" audit-routes)
    gateway_check(${prefix}controller_roundtrip "${platform}" comm facility_ATLA_control_host_0 facility_${remote}_control_host_0)
    gateway_check(${prefix}compute_roundtrip "${platform}" comm host_ATLA_small_1_compute_0 host_${remote}_small_1_compute_0)
    gateway_check(${prefix}storage_roundtrip "${platform}" comm host_ATLA_small_1_compute_0 facility_${remote}_shared_server)
    gateway_check(${prefix}remote_facility_io "${platform}" filesystem facility_${remote}_shared_filesystem facility_ATLA_control_host_0)
    gateway_check(${prefix}reverse_remote_facility_io "${platform}" filesystem facility_ATLA_shared_filesystem facility_${remote}_control_host_0)
    gateway_check(${prefix}remote_scratch_io "${platform}" filesystem cluster_${remote}_small_1_scratch_filesystem host_ATLA_small_1_compute_0)
    gateway_check(${prefix}shared_file_clients "${platform}" filesystem-clients facility_${remote}_shared_filesystem
                  facility_ATLA_control_host_0 host_${remote}_small_1_compute_0)
    gateway_check(${prefix}scratch_file_clients "${platform}" filesystem-clients cluster_${remote}_small_1_scratch_filesystem
                  host_ATLA_small_1_manager_0 host_${remote}_small_1_compute_1)
    add_test(NAME ${prefix}gateway_schema_validation COMMAND ${Python3_EXECUTABLE}
             ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_gateway_schema.py
             $<TARGET_FILE:platform_probe> $<TARGET_FILE:swarm_platform> ${platform})
    set_tests_properties(${prefix}gateway_schema_validation PROPERTIES TIMEOUT 30)
  endfunction()
  gateway_suite("" "${SWARM_TEST_PLATFORM}" DALL)
  gateway_suite(multihop_ "${SWARM_MULTIHOP_TEST_PLATFORM}" CLEM)
  gateway_check(multihop_shared_wan_contention "${SWARM_MULTIHOP_TEST_PLATFORM}" contention
                host_ATLA_small_1_compute_0 host_CLEM_small_1_compute_0
                host_ATLA_small_1_compute_1 host_CLEM_small_1_compute_1 fabric_CLEM_WASH
                --cfg=network/TCP-gamma:1073741824)
  gateway_suite(full_ "${SWARM_FULL_TEST_PLATFORM}" DALL)
  gateway_check(full_long_distance_roundtrip "${SWARM_FULL_TEST_PLATFORM}" comm
                facility_HAWI_control_host_0 facility_MASS_control_host_0)
  gateway_check(full_cross_facility_pair_contention "${SWARM_FULL_TEST_PLATFORM}" cross-pair-contention
                host_ATLA_small_1_compute_0 host_CLEM_small_1_compute_0
                host_DALL_small_1_compute_0 host_CLEM_small_1_compute_1 fabric_CLEM_WASH
                --cfg=network/TCP-gamma:1073741824)
  add_test(NAME full_remote_storage_coverage COMMAND ${Python3_EXECUTABLE}
           ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_full_platform.py
           --probe $<TARGET_FILE:platform_probe> --loader $<TARGET_FILE:swarm_platform>
           --platform ${SWARM_FULL_TEST_PLATFORM}
           --report-dir ${CMAKE_CURRENT_BINARY_DIR}/full-platform-coverage)
  set_tests_properties(full_remote_storage_coverage PROPERTIES TIMEOUT 180)
endif()
