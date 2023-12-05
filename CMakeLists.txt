cmake_minimum_required(VERSION 3.0.0)

project(lab12 VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)

add_subdirectory(bin)

add_subdirectory(lib)
link_directories(lib)