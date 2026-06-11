cmake_minimum_required(VERSION 3.20)

project(dkvs
    VERSION 0.1.0
    DESCRIPTION "Educational distributed key-value store in C++20"
    LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_library(dkvs_core
    src/kv_store.cpp
)

target_include_directories(dkvs_core
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_compile_options(dkvs_core
    PRIVATE
        $<$<CXX_COMPILER_ID:Clang,AppleClang,GNU>:-Wall -Wextra -Wpedantic>
)

add_executable(dkvs
    src/main.cpp
)

target_link_libraries(dkvs
    PRIVATE
        dkvs_core
)

