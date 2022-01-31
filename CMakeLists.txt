cmake_minimum_required(VERSION 3.21)
project(pmemlog C)

set(CMAKE_C_STANDARD 11)
link_libraries(pmemlog)

add_executable(pmemlog_exe main.c)
