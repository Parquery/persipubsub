cmake_minimum_required(VERSION 3.12)
project(persipubsub)

set(CMAKE_CXX_STANDARD 17)

include_directories(include)

enable_testing()
add_subdirectory(src)
add_subdirectory(test)
add_test(NAME myTest COMMAND Test)