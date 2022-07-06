
if (${CMAKE_BUILD_TYPE} STREQUAL "Debug")
set(HYRISE_DEBUG 1)
else()
set(HYRISE_DEBUG 0)
endif()

add_definitions(-DHYRISE_DEBUG=${HYRISE_DEBUG})

# Usually, you should not have to touch the following part ##########################################
cmake_minimum_required(VERSION 3.9 FATAL_ERROR)                                                     #
                                                                                                    #
project("Hyrise Tiering Selection Plugin")                                                                    #
                                                                                                    #
# Include CMakeLists.txt of Hyrise (all the build magic is contained there)                         #
add_subdirectory(hyrise-tiering/)                                                                           #
                                                                                                    #
# Add hyrise sources to be able to include headers                                                  #
include_directories(                                                                                #
    ${PROJECT_SOURCE_DIR}/hyrise-tiering/src/lib/                                                           #
    ${PROJECT_SOURCE_DIR}/hyrise-tiering/third_party/sql-parser/src
    ${PROJECT_SOURCE_DIR}/hyrise-tiering/src/benchmarklib/                                                #
    /home/Ben.Hurdelhey/local/include                                                #
    ${TBB_INCLUDE_DIR}                                                                              #
    ${Boost_INCLUDE_DIR}                                                                           #


    ${PROJECT_SOURCE_DIR}/hyrise-tiering/third_party/compact_vector/include                                 #
    ${PROJECT_SOURCE_DIR}/hyrise-tiering/third_party/flat_hash_map                                          #
    ${PROJECT_SOURCE_DIR}/hyrise-tiering/third_party/fsst                                                   #
    ${PROJECT_SOURCE_DIR}/hyrise-tiering/third_party/magic_enum/include                                     #
    ${PROJECT_SOURCE_DIR}/hyrise-tiering/third_party/nlohmann_json/single_include  # encoding config        #
    ${PROJECT_SOURCE_DIR}/hyrise-tiering/third_party/robin-map/include                                      #
    ${PROJECT_SOURCE_DIR}/hyrise-tiering/third_party/sql-parser/src                                         #
    ${PROJECT_SOURCE_DIR}/hyrise-tiering/third_party/uninitialized_vector                                   #
)

#
# Global flags and include directories                                                              #
add_compile_options(-std=c++17 -pthread -Wall -Wextra -pedantic -Werror -Wno-unused-parameter       #
-Wno-unknown-pragmas -Wno-unused-function)                                           #
                                                                                                    #
#####################################################################################################

include_directories(                                                                                #
    SYSTEM                                                                                          #
    ${TBB_INCLUDE_DIR}                                                                                                   #
    ${PROJECT_SOURCE_DIR}/hyrise-tiering/third_party/                                                       #
)


set(CMAKE_CXX_STANDARD_REQUIRED ON)                                                                 #
set(CMAKE_CXX_STANDARD 20)

add_definitions(-DHYRISE_WITH_UMAP=1)

### Modify this line to add your own plugin
add_plugin(NAME TieringSelectionPlugin SRCS tiering_selection_plugin.cpp tiering_selection_plugin.hpp tiering_calibration.hpp DEPS hyrise hyriseBenchmarkLib benchmark)
add_plugin(NAME WorkloadHandlerPlugin SRCS
    workload_handler.cpp
    workload_handler.hpp
    DEPS hyrise hyriseBenchmarkLib benchmark)


