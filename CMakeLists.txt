
if (${CMAKE_BUILD_TYPE} STREQUAL "Debug")
set(HYRISE_DEBUG 1)
else()
set(HYRISE_DEBUG 0)
endif()

add_definitions(-DHYRISE_DEBUG=${HYRISE_DEBUG})

# Usually, you should not have to touch the following part ##########################################
cmake_minimum_required(VERSION 3.9 FATAL_ERROR)                                                     #
                                                                                                    #
project("Hyrise Tiering Plugin")                                                                    #
                                                                                                    #
# Include CMakeLists.txt of Hyrise (all the build magic is contained there)                         #
add_subdirectory(hyrise-tiering/)                                                                           #
                                                                                                    #
# Add hyrise sources to be able to include headers                                                  #
include_directories(                                                                                #
    ${PROJECT_SOURCE_DIR}/hyrise-tiering/src/lib/                                                           #
    ${PROJECT_SOURCE_DIR}/hyrise-tiering/third_party/sql-parser/src                                         #
    ${TBB_INCLUDE_DIR}                                                                              #
    ${Boost_INCLUDE_DIR}                                                                           #
)                                                                                                   #
                                                                                                    #
# Global flags and include directories                                                              #
add_compile_options(-std=c++17 -pthread -Wall -Wextra -pedantic -Werror -Wno-unused-parameter       #
-Wno-dollar-in-identifier-extension -Wno-unknown-pragmas)                                           #
                                                                                                    #
#####################################################################################################

include_directories(                                                                                #
    SYSTEM                                                                                          #
                                                                                                    #
    ${PROJECT_SOURCE_DIR}/hyrise-tiering/third_party/                                                       #
)

set(CMAKE_CXX_STANDARD_REQUIRED ON)                                                                 #
set(CMAKE_CXX_STANDARD 20)


### Modify this line to add your own plugin
add_plugin(NAME TieringPlugin SRCS tiering_plugin.cpp tiering_plugin.hpp DEPS hyriseServer hyriseConsole)
