cmake_minimum_required(VERSION 3.20)
project(day_change_test LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_compile_options(-Wall -Wextra -Wpedantic)

find_package(Threads REQUIRED)

add_executable(DayChangeOvernight
        day_change_overnight.cpp
)
target_link_libraries(DayChangeOvernight PRIVATE Threads::Threads)

