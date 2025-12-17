cmake_minimum_required(VERSION 3.20)
project(DistributedTravelSystem)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Add Homebrew include and library directories for macOS
include_directories(/opt/homebrew/include)
link_directories(/opt/homebrew/lib)

# Create executables for each worker
add_executable(availability_worker availability_worker.cpp)
add_executable(pricing_worker pricing_worker.cpp)
add_executable(booking_worker booking_worker.cpp)

# Link libraries directly
target_link_libraries(availability_worker zmq pthread)
target_link_libraries(pricing_worker zmq pthread)
target_link_libraries(booking_worker zmq pthread)

# Installation
install(TARGETS availability_worker pricing_worker booking_worker
        RUNTIME DESTINATION bin)