cmake_minimum_required(VERSION 3.16)
project(swarm_extension_runner LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/../../conf/cmake")
include(CTest)

# Build the same repository-local loader that the Inspector checks use.
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/../loader" loader)
find_package(SimGrid 4.1 REQUIRED)
find_package(FSMod REQUIRED)
find_package(WRENCH REQUIRED)
find_package(nlohmann_json REQUIRED)
file(STRINGS "${WRENCH_INCLUDE_DIR}/wrench/simulation/Simulation.h" compat_marker
     REGEX "^#define WRENCH_SWARM_FSMOD_COMPAT_VERSION 1$")
set(SWARM_WRENCH_FSMOD_COMPAT FALSE)
if(compat_marker)
  set(SWARM_WRENCH_FSMOD_COMPAT TRUE)
endif()

add_executable(swarm_extension swarm_extension.cpp)
target_include_directories(swarm_extension PRIVATE
  ${WRENCH_INCLUDE_DIR} ${SimGrid_INCLUDE_DIR} ${FSMOD_INCLUDE_DIR})
target_link_libraries(swarm_extension PRIVATE
  WRENCH::WRENCH SimGrid::SimGrid FSMOD::FSMOD nlohmann_json::nlohmann_json ${CMAKE_DL_LIBS})
target_compile_definitions(swarm_extension PRIVATE
  SWARM_DEFAULT_LOADER="$<TARGET_FILE:swarm_platform>")
add_dependencies(swarm_extension swarm_platform)

if(BUILD_TESTING)
  find_package(Python3 COMPONENTS Interpreter REQUIRED)
  foreach(fixture IN ITEMS two_facilities multihop_facilities full)
    if(fixture STREQUAL "full")
      set(platform "${CMAKE_CURRENT_SOURCE_DIR}/../generated/fabric_platform.json")
    else()
      set(platform "${CMAKE_CURRENT_SOURCE_DIR}/../generated/${fixture}/fabric_platform.json")
    endif()
    if(SWARM_WRENCH_FSMOD_COMPAT)
      set(load_expectation --expect-compatible)
      set(load_test runner_${fixture}_loads_platform)
    else()
      set(load_expectation "")
      set(load_test runner_${fixture}_reports_jbod_incompatibility)
    endif()
    add_test(NAME ${load_test}
      COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_loading.py
        --runner $<TARGET_FILE:swarm_extension> --loader $<TARGET_FILE:swarm_platform>
        --probe $<TARGET_FILE:platform_probe> --platform ${platform}
        --report-dir ${CMAKE_CURRENT_BINARY_DIR}/checks/${fixture} ${load_expectation})
    set_tests_properties(${load_test} PROPERTIES TIMEOUT 30)
  endforeach()
  add_test(NAME runner_invalid_inputs
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_loading.py
      --runner $<TARGET_FILE:swarm_extension> --loader $<TARGET_FILE:swarm_platform>
      --platform ${CMAKE_CURRENT_SOURCE_DIR}/../generated/two_facilities/fabric_platform.json
      --report-dir ${CMAKE_CURRENT_BINARY_DIR}/checks/invalid-inputs --invalid-inputs)
  set_tests_properties(runner_invalid_inputs PROPERTIES TIMEOUT 30)
  add_test(NAME runner_minimal_wrench_loading_smoke
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_loading.py
      --runner $<TARGET_FILE:swarm_extension> --loader $<TARGET_FILE:swarm_platform>
      --platform ${CMAKE_CURRENT_SOURCE_DIR}/../generated/two_facilities/fabric_platform.json
      --report-dir ${CMAKE_CURRENT_BINARY_DIR}/checks/minimal-smoke --compatible-smoke)
  set_tests_properties(runner_minimal_wrench_loading_smoke PROPERTIES TIMEOUT 30)
  if(SWARM_WRENCH_FSMOD_COMPAT)
    add_test(NAME runner_memory_metadata COMMAND ${Python3_EXECUTABLE}
      ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_memory.py --runner $<TARGET_FILE:swarm_extension>
      --report-dir ${CMAKE_CURRENT_BINARY_DIR}/checks/memory-metadata)
    set_tests_properties(runner_memory_metadata PROPERTIES TIMEOUT 60)
    add_executable(wrench_storage_probe tests/wrench_storage_probe.cpp)
    target_include_directories(wrench_storage_probe PRIVATE
      ${WRENCH_INCLUDE_DIR} ${SimGrid_INCLUDE_DIR} ${FSMOD_INCLUDE_DIR})
    target_link_libraries(wrench_storage_probe PRIVATE
      WRENCH::WRENCH SimGrid::SimGrid FSMOD::FSMOD nlohmann_json::nlohmann_json ${CMAKE_DL_LIBS})
    add_dependencies(wrench_storage_probe swarm_platform)
    foreach(case IN ITEMS legacy-valid legacy-duplicate legacy-prefix legacy-bad-size unregistered-store
            extra-ordinary-disks null-binding duplicate-disk duplicate-store zero-capacity
            unregistered-filesystem empty-store mixed-hosts)
      add_test(NAME wrench_storage_validation_${case}
        COMMAND wrench_storage_probe $<TARGET_FILE:swarm_platform> validate ${case})
      set_tests_properties(wrench_storage_validation_${case} PROPERTIES TIMEOUT 30)
    endforeach()
    function(wrench_storage_check name platform filesystem writer reader)
      add_test(NAME ${name} COMMAND wrench_storage_probe $<TARGET_FILE:swarm_platform>
        io ${filesystem} ${writer} ${reader})
      set_tests_properties(${name} PROPERTIES ENVIRONMENT "PLATFORM_CONFIG=${platform}" TIMEOUT 30)
    endfunction()
    wrench_storage_check(wrench_two_facilities_shared_io "${SWARM_TEST_PLATFORM}"
      facility_DALL_shared_filesystem facility_ATLA_control_host_0 host_ATLA_small_1_compute_0)
    wrench_storage_check(wrench_two_facilities_scratch_io "${SWARM_TEST_PLATFORM}"
      cluster_DALL_small_1_scratch_filesystem facility_ATLA_control_host_0 host_ATLA_small_1_compute_0)
    wrench_storage_check(wrench_multihop_shared_io "${SWARM_MULTIHOP_TEST_PLATFORM}"
      facility_CLEM_shared_filesystem facility_ATLA_control_host_0 host_ATLA_small_1_compute_0)
    wrench_storage_check(wrench_multihop_scratch_io "${SWARM_MULTIHOP_TEST_PLATFORM}"
      cluster_CLEM_small_1_scratch_filesystem facility_ATLA_control_host_0 host_ATLA_small_1_compute_0)
    wrench_storage_check(wrench_full_shared_io "${SWARM_FULL_TEST_PLATFORM}"
      facility_MASS_shared_filesystem facility_HAWI_control_host_0 host_ATLA_small_1_compute_0)
    wrench_storage_check(wrench_full_scratch_io "${SWARM_FULL_TEST_PLATFORM}"
      cluster_MASS_small_1_scratch_filesystem facility_HAWI_control_host_0 host_ATLA_small_1_compute_0)
  endif()
endif()
